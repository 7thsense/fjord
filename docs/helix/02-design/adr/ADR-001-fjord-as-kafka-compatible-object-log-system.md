---
ddx:
  id: adr-fjord-as-kafka-compatible-object-log-system
  depends_on:
    - product-vision
    - prd
    - concerns
    - research-prior-art
---

# ADR-001: fjord as a Kafka-Compatible Object-Log System

## Status

Proposed

## Context

The original fjord framing treated the project as a light Kafka interface over
`object-log`. That is too narrow. A useful Kafka-facing system must satisfy
Kafka producer and consumer workflows, including protocol negotiation,
metadata, leader routing or equivalent errors, produce, fetch, committed
offsets, consumer groups, and eventually idempotent producer and transaction
semantics.

Prior art supports the broader framing:

- WarpStream uses stateless Kafka-protocol agents with object storage and a
  separate metadata/control plane. Its protocol design manipulates Kafka
  Metadata responses for zone-aware routing and load balancing because clients
  expect broker/leaders even when any agent can serve requests.
- AutoMQ stores Kafka data in object storage and treats leader epoch and
  producer snapshots as part of the storage architecture. It differs from
  WarpStream by using a WAL durability stage before object upload.
- Bufstream presents a Kafka-compatible object-storage-backed service with a
  metadata backend.
- Kafka Diskless Topics validate object-storage-backed active topics as a
  direction inside the Kafka ecosystem.

fjord should therefore be a Kafka-compatible service, while `object-log` remains
the reusable embeddable log core.

The strategic risk is direct overlap with WarpStream. If fjord requires a
hosted metadata service, cannot keep durable state in object storage or
self-hosted components, and does not produce a reusable object-log core for
pqueue/Niflheim, there is little reason to build it.

## Decision

fjord will be designed as a Kafka-compatible streaming system whose durable log
data is stored exclusively through `object-log` into S3-compatible object
storage.

The boundary is:

- `object-log` owns reusable log mechanics: records, batches, partitions,
  offsets, segment encoding, manifests, object-store durability, checksums,
  replay, and backend-neutral log traits.
- fjord owns Kafka service mechanics: wire protocol, API version support,
  metadata responses, client-visible leadership, produce/fetch routing,
  consumer groups, committed offsets, idempotent producer state, transaction
  state, admin APIs, security, metrics, caches, and compatibility tooling.

fjord MAY require a metadata/control-plane backend. That backend is not
considered part of durable log data, but it is part of Kafka compatibility and
must be designed explicitly. Local broker disk is cache only and MUST NOT be the
durable authority for acknowledged records.

The first design checkpoint MUST be a build/no-build decision against
WarpStream-class systems. fjord proceeds only if it can preserve a materially
different envelope: open/self-hostable, no required hosted control plane,
object-storage-first durability, direct object-log reuse, and a narrower
latency-tolerant initial scope.

## Consequences

### Positive

- The project goal matches what users expect from "Kafka-compatible."
- Standard Kafka clients and tooling can validate real behavior.
- pqueue and Niflheim can still depend on `object-log` directly, while fjord
  becomes a third consumer that pressure-tests the same core contract.
- Operational simplicity remains central: service nodes can be replaceable
  because durable record data is in object storage.
- The project has an explicit stop condition if differentiation cannot survive
  design.

### Negative

- fjord is larger than a protocol shim; consumer groups, offsets, metadata, and
  transactions are substantial distributed-systems surfaces.
- A metadata/control-plane dependency is likely unavoidable for full Kafka
  compatibility.
- Object-storage latency and object operation costs require careful batching,
  indexing, caching, and compaction.
- A strict "S3-only durable metadata" goal may make consumer groups,
  coordinator state, and transactions substantially harder.

### Constraints

- Do not claim full Kafka compatibility until the relevant protocol APIs and
  client workflows pass conformance and fault tests.
- Do not store acknowledged record data durably on local broker disk.
- Do not duplicate object-log segment/manifest/replay mechanics inside fjord.
- Do not bake pqueue or Niflheim domain semantics into fjord.

## Alternatives Considered

### Thin Adapter over object-log

Rejected as the product framing. It could help test produce-like semantics but
would not satisfy Kafka consumers, metadata, groups, or offsets.

### Kafka Connect Sink to S3

Rejected. It exports Kafka data to S3 but does not make object storage the
primary durable log substrate.

### Classic Kafka Tiered Storage

Rejected as the target. Tiered storage keeps broker-local active log authority;
fjord's purpose is object-storage-first durable data.

### Fork Kafka and Add Diskless Topics

Deferred. It may be a future comparison point, but fjord's near-term value is
to build around `object-log` and test an embeddable object-log core.

## Open Follow-Up ADRs

1. Metadata/control-plane backend and consistency model — **ADR-004**
   (accepted as direction; confirmation gated on SPIKE-001).
2. Client-visible leader/follower and any-node routing model — **ADR-003**
   (accepted).
3. Fetch index, cache, and object-read strategy — open.
4. Consumer group coordinator and committed offset storage — designed in
   **TD-004** (no separate ADR needed; decisions recorded in ADR-004/TD-004).
5. Idempotent producer and transaction state model — open.
6. Retention, compaction, and object-log segment lifecycle — open (interacts
   with object-log M2 and ADR-004 internal-topic compaction).
7. Security: TLS, SASL, ACLs, and multi-tenant isolation — open (L1 plumbing
   vs L3 enforcement intent recorded in API-001 principle 6).
8. Strategic differentiation review versus WarpStream, AutoMQ, Bufstream, and
   Kafka Diskless Topics — operationalized in the build/no-build validation
   checklist (first review before M3).
