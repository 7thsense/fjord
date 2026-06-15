---
ddx:
  id: td-metadata-routing-coordination
  depends_on:
    - prd
    - feature-registry
    - adr-fjord-as-kafka-compatible-object-log-system
    - api-kafka-compatibility-surface
---

# Technical Design: TD-002 Metadata, Routing, and Coordination

> **Superseded by ADR-008 / COORD-001 / TD-007 (2026-06-15).** This TD framed the
> metadata/control plane as an open choice and recorded the interim decisions
> (emulated single leader per ADR-003; object-log internal topics as the primary
> durable metadata path per ADR-004). Both are superseded: metadata, sequencing,
> routing, group/offset/producer state, and membership now live in the **pluggable
> central coordinator** (default self-hosted Postgres; COORD-001), with **stateless
> brokers** and the Metadata "leader" as a routing/cache-locality hint (ADR-008).
> Read **COORD-001** for the coordinator interface and **TD-007** for the
> Kafka-semantics state. This document is retained for the candidate-backend survey
> and the historical reasoning only.

## Scope

Kafka compatibility needs durable state beyond log bytes. This design frames
the control-plane responsibilities and the open decision about where durable
metadata lives.

## State Surfaces

| Surface | Durable Authority Required | Notes |
|---------|----------------------------|-------|
| topics and partitions | Yes | create/delete/config/list/describe |
| node membership | Yes or leased | drives Metadata responses |
| partition owner/leader epoch | Yes | clients route by leader and retry stale metadata |
| producer id/epoch/sequence snapshots | Yes for idempotence | can live in object-log metadata records |
| consumer group membership | Yes | coordinator correctness |
| committed offsets | Yes | must survive node loss |
| ACLs/auth config | P3 | production readiness |

## Candidate Metadata Backends

| Backend | Fit | Concern |
|---------|-----|---------|
| object-log internal topics | Strong alignment with S3-only durability | Coordinator latency and compaction design needed |
| object storage manifest objects | Simple operational model | CAS contention and read amplification |
| self-hosted Postgres | Simple and reliable small-scale control plane | Violates strict S3-only metadata if made mandatory |
| etcd/ZooKeeper | Known coordination pattern | Conflicts with operational simplicity goal |
| hosted metadata service | WarpStream-like | Not acceptable as required core dependency |

## Direction

**Decided by ADR-004 (gated on SPIKE-001)**: object-log internal topics are
the primary durable metadata path. Postgres exists only as the spike-failure
fallback — an optional self-hosted mode behind the same metadata-plane
interface, never a required dependency. etcd/ZooKeeper are rejected for the
core product. SPIKE-001 must pass its latency bars before M4 implements the
durable backend; M4 builds against an in-memory implementation of the
metadata-plane interface in the meantime.

## Leader Model

**Decided by ADR-003**: emulated single leader per partition for L1/L2.

- Metadata assigns exactly one owner node per topic partition; replica/ISR
  lists carry only that owner.
- Non-owner nodes return `NOT_LEADER_OR_FOLLOWER`; they do not proxy or
  redirect. Clients refresh metadata and reroute.
- Leader epoch changes are persisted before clients are told about a new owner.
- Object-log manifest ordering remains authoritative for committed records.
- Any-node serving with reshaped Metadata is a post-L2 routing optimization.

## Testing

- Metadata response tests for topic, missing topic, stale leader, and reassigned
  leader.
- Node-loss tests proving no acknowledged record or committed offset depends on
  local disk.
- Conflict tests proving only one metadata transition wins under concurrent
  writers.

