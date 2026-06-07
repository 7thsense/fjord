---
ddx:
  id: implementation-plan
  depends_on:
    - feature-registry
    - td-kafka-protocol-gateway
    - td-metadata-routing-coordination
    - td-object-log-data-plane
    - tp-kafka-compatibility-and-performance
    - tp-implementation-increments
---

# Implementation Plan

## Build Order

Fjord depends on object-log for durable data-plane semantics, but not every
Fjord task must wait. Protocol scaffolding, compatibility fixtures, and metadata
design can begin while object-log hardens.

## Milestones

### M1: Protocol Gateway Skeleton

- Create Rust service crate.
- Implement Kafka frame IO, header version selection, handler registry, and
  ApiVersions.
- Add Metadata skeleton with a static single-node topic registry.
- Add Niflheim-informed connection reader/writer split.
- Gate: unit tests plus `kcat -L` against local server.

### M2: Shared Wire Extraction Decision

- Compare Fjord skeleton with Niflheim's protocol module.
- Extract a product-neutral `kafka-wire` crate only if the boundary is stable.
- Gate: Fjord and an extraction spike can use the same frame/version/registry
  code without object-log or Niflheim dependencies.

### M3: object-log Produce/Fetch

- Integrate object-log as the durable append/read backend.
- Implement Produce and Fetch for assigned partitions.
- Map `acks` semantics explicitly.
- Gate: Java client produce/fetch round trip with durable object-log commit.

### M4: Metadata and Routing

- Implement topic/partition metadata state.
- Implement synthetic leader/owner routing and stale metadata behavior.
- Gate: clients reroute or retry correctly after reassignment and node loss.

### M5: Consumer Groups and Offsets

- Implement FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
  OffsetCommit, and OffsetFetch for the first compatibility level.
- Gate: Java consumer group can commit offsets, restart, and rebalance after
  node loss.

### M6: Operations and Performance

- Add metrics, config validation, runbooks, fault harness, and performance
  profiles.
- Gate: Kafka performance tools produce reproducible throughput/latency/cost
  evidence for the declared profile.

## Dependency Notes

- M1 can start immediately.
- M2 can start after M1 has enough code to prove the shared API.
- M3 waits for object-log conformance and object backend hardening.
- M4 can start with an in-memory metadata backend but cannot claim durability
  until TD-002's backend decision is implemented.
- M5 waits for M4.
- M6 runs throughout but only becomes a production gate after M3-M5.

## Exit Criteria

- Every supported Kafka API/version appears in the compatibility matrix.
- Produce/fetch data is durable only through object-log/object storage.
- Local node disk is cache only.
- Standard Kafka clients pass the declared compatibility level.
- Build/no-build review still shows meaningful differentiation from
  WarpStream-class systems.

