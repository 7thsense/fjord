---
ddx:
  id: feature-registry
  depends_on:
    - prd
    - concerns
---

# Feature Registry

## FJORD-FEAT-001: Kafka Protocol Gateway

Expose the Kafka TCP protocol for supported API versions. Covers framing,
ApiVersions, Metadata, Produce, Fetch, ListOffsets, errors, SASL/TLS hooks, and
client compatibility fixtures.

## FJORD-FEAT-002: object-log Durable Data Plane

Append and fetch Kafka records through object-log: stateless brokers multiplex
many partitions into L0 objects, then a coordinator transaction assigns offsets
(TD-005). Durable acknowledgement = object PUT durable AND coordinator commit;
local node disk is cache only.

## FJORD-FEAT-003: Central Coordinator and Routing Control Plane

The pluggable, self-hosted central coordinator (default Postgres; etcd/Dragonfly
behind COORD-001; object-log internal topics optional) owns sequencing, topics,
partitions, leader epochs, group/offset/producer state, and broker membership
(ADR-008, COORD-001, TD-007). Brokers are stateless; the Metadata "leader" is a
routing/cache-locality hint. No hosted control plane.

## FJORD-FEAT-004: Consumer Groups and Offsets

Implement FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup,
OffsetCommit, and OffsetFetch for supported clients. Group and offset state live
in the coordinator and survive node loss (TD-007: coordinator = single owner of
the groups shard for `hash(group)`, monotonic generation enforced by the store).

## FJORD-FEAT-005: Fetch Indexes and Cache

Serve Fetch efficiently from object-log segment manifests, indexes, local cache,
and prefetch. Cache loss must not affect correctness.

## FJORD-FEAT-006: Operations and Observability

Expose configuration, metrics, runbooks, fault tests, and cost/performance
profiles for object-storage-backed Kafka workloads.

## FJORD-FEAT-007: Build/No-Build Differentiation

Continuously compare Fjord with WarpStream, AutoMQ, Bufstream, and Kafka
Diskless Topics. Stop or redirect the project if Fjord loses its open,
self-hostable, object-log-reusable differentiation. Pass criteria, cadence,
and evidence form are defined in the build/no-build validation checklist;
first review completes before M3.

## FJORD-FEAT-008: Transactions and Exactly-Once

Idempotent producers and full transactional EOS (InitProducerId,
AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn, read_committed).
Designed in TD-008: `end_txn` is one coordinator transaction (decision +
offset-flip + LSO advance synchronous; marker materialization async). On the
Kafka parity surface (API-001 capability matrix: Accept).

