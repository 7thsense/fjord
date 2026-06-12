---
ddx:
  id: td-consumer-group-coordinator
  depends_on:
    - adr-client-visible-leader-model
    - adr-durable-metadata-path
    - td-metadata-routing-coordination
    - api-kafka-compatibility-surface
    - prd
---

# Technical Design: TD-004 Consumer Group Coordinator

## Scope

Design the group coordinator behind FindCoordinator, JoinGroup, SyncGroup,
Heartbeat, LeaveGroup, OffsetCommit, and OffsetFetch for L2 (FEAT-004,
implementation slice M5). Targets the **classic consumer group protocol**
(eager and cooperative client-side assignors work unchanged because the
coordinator is assignment-opaque). The KIP-848 next-generation group protocol
is out of scope until after L2.

## Technical Approach

### Coordinator placement

Each group maps to a partition of the internal metadata topic
`__fjord_groups` by `hash(group.id) % partitions`. The **coordinator for a
group is the owner (ADR-003 leader) of that partition**; FindCoordinator
returns that node. Coordinator failover is therefore ordinary partition
ownership reassignment in the metadata plane (TD-002), and clients recover
via `NOT_COORDINATOR` plus rediscovery — no separate election mechanism.

### Group state machine

In-memory per-group state on the coordinator follows Kafka's lifecycle:
`Empty → PreparingRebalance → CompletingRebalance → Stable` (and `Dead` on
removal). Generation increments on each completed rebalance. Member ids are
coordinator-assigned. Session and rebalance timers (heartbeat/session
timeout, `rebalance.timeout.ms`) are in-memory only — timers are never
durable state.

### Durability boundary

Durable group state is written to `__fjord_groups` via object-log (ADR-004)
at exactly these transitions:

| Event | Durable record |
|-------|----------------|
| Rebalance completes (SyncGroup served) | group metadata: generation, protocol, members, assignments |
| OffsetCommit | committed offsets for the group's topic partitions |
| Group becomes Empty/Dead | tombstone for compaction |

OffsetCommit responses return only after the object-log durable boundary
(`AckMode::All`), satisfying FR-15/FR-17. Joins, heartbeats, and in-flight
rebalance progress are **not** durable; a coordinator loss during a rebalance
restarts the rebalance at the new coordinator, which is Kafka-compatible
client-visible behavior.

### Failover and replay

A new owner of a `__fjord_groups` partition rebuilds the compacted view
(latest group metadata + latest offsets per group-partition key) by replaying
the internal topic before serving group APIs for those groups. Replay time is
bounded by the SPIKE-001 takeover bar; snapshot/compaction of internal topics
follows object-log M2 retention work.

### Contract references

| Surface | Governing contract | Usage here |
|---------|--------------------|-----------|
| Group/offset API request/response semantics | API-001 (successor normative contract per its §Purpose) | Coordinator serves the listed L2 APIs at the flexible-version floor |
| Durable append/read, ack boundary, replay | object-log CONTRACT-001 | Internal topic records use `AckMode::All`; replay rebuilds coordinator state |

## Component Changes

| Component | Change |
|-----------|--------|
| Gateway registry (TD-001) | Register the seven group API handlers at L2 versions |
| Metadata plane (TD-002) | Reserve `__fjord_groups`; expose owner lookup for FindCoordinator |
| Coordinator module (new) | Group state machines, member/session timers, rebalance sequencing |
| Offset store (new) | Compacted in-memory offsets view + durable commit path through object-log |
| Data plane (TD-003) | None — group state never touches user-topic partitions |

## Testing

- State-machine unit tests: join/sync/heartbeat/leave sequences, generation
  fencing, illegal-generation and unknown-member errors, timer expiry.
- TP-001 T6 (commit survives node loss), T7 (rebalance convergence), plus
  `NOT_COORDINATOR` rediscovery after ownership reassignment.
- TP-002 "Group coordinator" gate: Java consumer group commit/restart/
  rebalance against a running node, then against coordinator takeover.
- Replay tests at the SPIKE-001 history sizes; offsets and generation match
  pre-failover state.
- `kafka-consumer-groups.sh` describe/list smoke once admin surface allows.
