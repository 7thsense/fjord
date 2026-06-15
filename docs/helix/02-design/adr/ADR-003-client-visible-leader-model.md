---
ddx:
  id: adr-client-visible-leader-model
  depends_on:
    - adr-fjord-as-kafka-compatible-object-log-system
    - api-kafka-compatibility-surface
    - research-prior-art
    - prd
---

# ADR-003: Client-Visible Leader Model

## Status

Accepted (operator decision, 2026-06-12); amended by ADR-005 (2026-06-14).

> **Amendment (superseded by ADR-008, 2026-06-15).** The current model is
> **stateless brokers + a central coordinator** (ADR-008): brokers hold no durable
> data and any broker serves any partition; the **coordinator** is the sequencing
> authority (offsets assigned in the coordinator transaction, COORD-001), not a
> per-partition broker leader. The Metadata "leader" fjord presents is a
> **routing/cache-locality hint** for load balancing, and `NOT_LEADER_OR_FOLLOWER`
> survives only as a routing convention (a broker may redirect a client to a
> better-located peer). The intermediate ADR-005 "leaderless write / offsets at
> metadata-commit" and ADR-007 "single-writer-per-shard owner" framings are both
> superseded by the coordinator model.

## Context

Kafka clients route Produce and Fetch by the leader assignments in Metadata
responses and refresh metadata on routing errors. fjord's internals are
stateless service nodes over object storage, so client-visible leadership is
an emulation choice, not a storage fact. API-001 framed three options:
emulated leader, any-node service with synthetic leaders, and a leaderless
protocol extension. The choice blocks Produce, Fetch, ListOffsets,
OffsetForLeaderEpoch, and the M1 Metadata skeleton.

WarpStream demonstrates that Metadata reshaping for any-node, zone-aware
routing works with standard clients, but it carries a larger design and test
surface: request proxying or rerouting, zone-locality policy, and
load-balancer interaction all become L1 concerns.

## Decision

For L1 and L2, fjord uses an **emulated single leader per partition**:

- Metadata assigns exactly one owner node as leader for each topic partition.
  The replica and ISR lists contain only that owner (placeholder semantics).
- Only the owner serves Produce, Fetch, ListOffsets, and
  OffsetForLeaderEpoch for its partitions.
- Non-owner nodes return `NOT_LEADER_OR_FOLLOWER`; clients refresh metadata
  and reroute. Non-owners do not proxy and do not silently serve.
- Leader epoch increments are persisted in the metadata plane before the new
  owner appears in any Metadata response (per TD-002).
- Owner assignment is a routing fact only. Durable record data remains in
  object storage through object-log; any node could be made owner without
  copying partition data.

Any-node serving with reshaped or zone-aware Metadata responses is **deferred
as a routing optimization** to revisit after L2, behind the same protocol
surface. The decision constrains client-visible behavior, not internals.

## Consequences

### Positive

- Simplest correct routing story for L1: one authority per partition, standard
  client retry semantics, no proxy path to design or test.
- Stale-routing behavior is exactly Kafka's: tests can assert
  `NOT_LEADER_OR_FOLLOWER` plus metadata refresh (TP-001 T2, T12).
- Ownership transfer is cheap (metadata-only), preserving FR-28 node
  replaceability.

### Negative

- Load balancing is coarse: partition granularity, no zone-aware routing, so
  cross-AZ client traffic is unoptimized until the any-node follow-up.
- A hot partition concentrates load on its owner node.
- Follower fetch (KIP-392) is out of scope; the replica list never offers
  alternatives.

## Alternatives Considered

### Any-node service with synthetic/zone-aware leaders (WarpStream-style)

Deferred, not rejected. It is the likely end state for cost and zone-locality,
but it adds proxying/rerouting, zone policy, and load-balancer behavior to the
L1 critical path. The emulated-leader surface is forward-compatible with it.

### Leaderless protocol-compatible extension

Rejected. It depends on nonstandard client behavior, contradicting the
compatibility principle that standard clients and tools are the measure
(API-001 §Compatibility Principles).

### Decide via tech spike

Rejected as unnecessary. The spike would compare client behavior across
options, but the emulated-leader model is the only option whose L1 cost is
already known to be small, and it does not foreclose the others.
