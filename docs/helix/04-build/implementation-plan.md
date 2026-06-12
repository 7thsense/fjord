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

## Scope

Fjord depends on object-log for durable data-plane semantics, but not every
Fjord task must wait. Protocol scaffolding, compatibility fixtures, and metadata
design can begin while object-log hardens.

## Implementation Slices

Slices are labeled M1-M6 and referenced by those labels across fjord docs.

### M1: Protocol Gateway Skeleton

- Create Rust service crate.
- Implement Kafka frame IO, header version selection, handler registry, and
  ApiVersions.
- Add Metadata skeleton with a static single-node topic registry, emitting
  ADR-003 emulated-leader responses (one owner per partition).
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

- Implement topic/partition metadata state behind the metadata-plane
  interface (in-memory first; durable backend per ADR-004 after SPIKE-001
  passes).
- Implement ADR-003 owner routing and stale metadata behavior
  (`NOT_LEADER_OR_FOLLOWER`, epoch persisted before announcement).
- Until CreateTopics lands at L3, topics for M3-M5 work are created through a
  bootstrap seam (declarative topic config applied at startup); the seam is
  test/ops tooling, not Kafka API surface.
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

## Issue Decomposition

Tracked work lives in `.ddx/beads.jsonl`:

| Bead | Milestone / Gate |
|------|------------------|
| `fjord-66bad250` Complete Fjord full-scope delivery | Epic for M1-M6 |
| `fjord-08961019` Bootstrap Kafka protocol gateway skeleton | M1 |
| `fjord-80ddb508` Decide and prototype shared kafka-wire extraction | M2 |
| `fjord-15369989` Integrate object-log for durable Produce and Fetch | M3 |
| `fjord-f8989544` Implement Fjord metadata and routing prototype | M4 |
| `fjord-6ab8369e` Implement consumer groups and offset state | M5 |
| `fjord-e5600c21` Add Fjord compatibility and performance harness | M6 |
| `fjord-42864fe0` Run Fjord build/no-build differentiation review | Standing gate (FEAT-007); first checkpoint before M3 |

## Risks and Rollbacks

- **Differentiation fails review**: the build/no-build bead is the standing
  rollback for the whole plan — stop or redirect before M3 rather than after
  storage integration sinks cost.
- **object-log hardening slips** (S3 adapter, retention, conformance — its
  M1-M4): M1/M2/M4 protocol and metadata work proceeds against traits and
  in-memory backends; M3 is the only milestone blocked.
- **Shared-crate extraction proves premature at M2**: keep scaffolding in-repo
  and retry after M3; ADR-002 already gates extraction on a stable boundary.
- **Metadata backend choice invalidated at M4**: TD-002 requires the in-memory
  backend boundary to match the durable design, so a backend swap replaces a
  module, not the routing design.
- Each milestone gate is a test, not a claim; a failed gate rolls the milestone
  back to design (`evolve` the governing TD) instead of patching forward.

## Validation Plan

Every slice gate is defined in TP-002 (increment gates) and, for compatibility
claims, TP-001 (client/tool conformance, performance profiles, fault matrix).
A slice is complete only when its TP-002 gate passes with recorded evidence:
commands, client versions, object-log version, and backend modes.

## Dependency Notes

- M1 can start immediately.
- M2 can start after M1 has enough code to prove the shared API.
- M3 waits for object-log conformance and object backend hardening. fjord
  consumes object-log as a path/git dependency pinned to a recorded commit
  SHA until object-log publishes versions; every TP evidence record cites the
  pinned SHA.
- M4 can start with an in-memory metadata backend but cannot implement or
  claim durable metadata until SPIKE-001 passes the ADR-004 latency bars.
- SPIKE-001 can run any time after object-log's local/S3-compatible stores
  are usable; it should complete before M4's durable backend work begins.
- M5 waits for M4 and is governed by TD-004.
- The first build/no-build review (validation checklist, bead
  `fjord-42864fe0`) completes before M3.
- M6 runs throughout but only becomes a production gate after M3-M5.

## Exit Criteria

- Every supported Kafka API/version appears in the compatibility matrix.
- Produce/fetch data is durable only through object-log/object storage.
- Local node disk is cache only.
- Standard Kafka clients pass the declared compatibility level.
- Build/no-build review still shows meaningful differentiation from
  WarpStream-class systems.

