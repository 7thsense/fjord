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

Fjord should prefer object-log internal topics for durable metadata if group and
offset latency can be made acceptable. Postgres may be supported as an optional
self-hosted small-scale control-plane mode, but not as a required hosted data
plane dependency. etcd/ZooKeeper are fallback references, not preferred
requirements.

## Leader Model

Kafka clients expect broker leaders. Fjord can expose synthetic leaders while
internally serving from any eligible node:

- Metadata assigns a topic partition to a node for client routing.
- Non-owner nodes either proxy, return `NOT_LEADER_OR_FOLLOWER`, or redirect
  by updated Metadata according to the chosen mode.
- Leader epoch changes are persisted before clients are told about a new owner.
- Object-log manifest ordering remains authoritative for committed records.

## Testing

- Metadata response tests for topic, missing topic, stale leader, and reassigned
  leader.
- Node-loss tests proving no acknowledged record or committed offset depends on
  local disk.
- Conflict tests proving only one metadata transition wins under concurrent
  writers.

