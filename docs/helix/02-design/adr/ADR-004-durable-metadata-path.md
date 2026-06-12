---
ddx:
  id: adr-durable-metadata-path
  depends_on:
    - adr-fjord-as-kafka-compatible-object-log-system
    - td-metadata-routing-coordination
    - api-kafka-compatibility-surface
    - prd
---

# ADR-004: Durable Metadata Path

## Status

Accepted as direction (operator decision, 2026-06-12); confirmation gated on
SPIKE-001 results before the M4 backend implementation.

## Context

Kafka compatibility requires durable state beyond log bytes: topics,
partitions, leader/partition epochs, node membership, consumer group
membership, committed offsets, and producer snapshots (TD-002 §State
Surfaces). fjord's differentiation gates (PRD §Critical Differentiation)
require no hosted metadata service and prefer S3-compatible object storage
for durable state wherever Kafka semantics allow. TD-002 surveyed candidate
backends but recorded only a preference, and the deciding evidence —
coordinator and offset-commit latency over object-log internal topics — does
not exist yet.

## Decision

1. **Primary path: object-log internal topics.** Durable metadata (topics,
   epochs, membership transitions, group state, committed offsets, producer
   snapshots) is stored as records in reserved internal object-log topics
   (e.g. `__fjord_metadata`, `__fjord_groups`), replayed into in-memory state
   on node start or ownership change. This keeps all durable state in
   S3-compatible object storage and exercises object-log as a real consumer.
2. **SPIKE-001 must validate the latency bars before M4 implements the
   durable backend.** The spike's bars and method are owned by SPIKE-001;
   headline bars: committed offset commit p99 ≤ 500 ms, metadata transition
   (epoch bump, topic create) p99 ≤ 2 s, group state replay on coordinator
   takeover ≤ 5 s for the reference group size.
3. **Fallback: optional self-hosted Postgres mode**, designed behind the same
   metadata-plane interface, is implemented **only if** SPIKE-001 fails the
   bars. It is an explicit, documented exception to S3-only durability and is
   never a hosted-service requirement.
4. **Never:** a required hosted metadata/control-plane service. If both the
   primary path and the self-hosted fallback prove unworkable, that is a
   build/no-build signal, not a license to add a hosted dependency.
5. etcd/ZooKeeper remain rejected for the core product (operational
   simplicity gate, TD-002 §Candidate Metadata Backends).

## Consequences

### Positive

- Strongest form of the S3-first differentiation: data plane and control
  plane share one durable substrate and one operational dependency.
- object-log gains a demanding second workload (small, latency-sensitive,
  compaction-hungry records), pressure-testing the core per the vision.
- The metadata-plane interface is decided now, so M4 can build against an
  in-memory implementation while the spike runs.

### Negative

- Coordinator-shaped workloads are a known poor fit for raw object storage
  latency; the spike may fail and cost a backend redesign cycle.
- Internal topics need compaction/snapshot design earlier than the data
  plane otherwise requires (interacts with object-log M2 retention work).
- Replay-on-takeover couples coordinator failover time to object read
  latency; bars must include takeover, not just steady state.

## Alternatives Considered

### Self-hosted Postgres first, migrate to object-log topics later

Rejected as the default. It ships sooner but weakens the differentiation
story at first contact, and migration off a relational control plane rarely
happens once shipped. Retained only as the spike-failure fallback.

### Object-storage manifest/CAS objects directly (no internal topics)

Rejected for group/offset state: per-key CAS objects reintroduce the
tiny-object write pattern FR-24 forbids and offer no ordered replay. Manifest
CAS remains object-log's own commit mechanism, which the internal-topic path
inherits.

### etcd/ZooKeeper

Rejected: conflicts with the operational-simplicity goal and adds a second
stateful system to operate (TD-002).

### Required hosted metadata service (WarpStream-style)

Rejected by the build gate (PRD FR-32): violates the core differentiation.
