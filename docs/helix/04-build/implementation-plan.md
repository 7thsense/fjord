---
ddx:
  id: implementation-plan
  depends_on:
    - feature-registry
    - adr-pluggable-central-coordinator
    - coord-coordinator-store-contract
    - td-kafka-protocol-gateway
    - td-multiplexed-write-path-and-sequencing
    - td-fetch-read-path-and-cache
    - td-metadata-plane-state-and-kafka-semantics
    - td-transactions-and-exactly-once
    - td-object-log-data-plane
    - tp-kafka-compatibility-and-performance
    - tp-implementation-increments
    - tp-verification-strategy-oracles-and-properties
---

# Implementation Plan

## Scope

fjord = stateless brokers + object storage (record data) + a pluggable
self-hosted central coordinator (default Postgres; ADR-008, COORD-001). The plan
builds on the heimq engine crates (`heimq-wire`/`heimq-broker` for frame IO and
handler scaffolding) with fjord owning sequencing above object_log's
`ObjectStore`. SPIKE-001 (now: coordinator commit latency/throughput per backend)
is retired first, because the whole produce floor depends on it.

## Implementation Slices

Slices are labeled M1-M7 and referenced by those labels across fjord docs. The
milestone structure reflects the central-coordinator design (ADR-008); the
earlier object-log-sequencer / emulated-leader milestones are superseded.

### M0 (SPIKE-001): Coordinator latency/throughput spike — run first

- Prototype `commit_object`/`end_txn` against the default Postgres backend (and
  characterize etcd/Dragonfly); measure p50/p99/throughput vs the produce floor.
- Gate (build/no-build-adjacent): Postgres clears the coordinator-latency budget;
  if no self-hosted backend does, escalate per ADR-004/ADR-008.

### M1: Protocol Gateway Skeleton

- Create the broker service crate on `heimq-wire`/`heimq-broker`.
- Implement Kafka frame IO, header version selection, handler registry, ApiVersions.
- Metadata skeleton presenting a routing/cache-locality leader hint (ADR-008);
  any broker serves any partition.
- Gate: unit tests plus `kcat -L` against local server.

### M2: CoordinatorStore + Postgres backend (foundational)

- Implement the `CoordinatorStore` trait (COORD-001) and the **default Postgres
  backend** with the reference schema (partition_state, object_index,
  producer_state/seq, group_state, committed_offsets, txn_state, txn_partition,
  aborted_ranges, leader_epoch_history, brokers); pre-insert partition_state rows
  on create_topic (B2).
- Implement `commit_object` with the mandated lock order (B3) and capability
  gating; pass the COORD-001 conformance suite (O7).
- Gate: conformance suite green on Postgres (linearizable commit_object, lease
  fencing, durability-survives-restart).

### M3: Write/read path on object-log + coordinator

- Stateless broker produce path: buffer → multiplexed L0 PUT → `commit_object`
  (TD-005); Fetch via the coordinator index + locality-aware cache (TD-006).
- Map `acks` explicitly; idempotent-producer fencing (TD-007).
- Gate: Java client produce/fetch round trip; durable ack = PUT + coordinator
  commit; differential vs real Kafka (O1) on the produce/fetch surface.

### M4: Consumer Groups and Offsets

- FindCoordinator/Join/Sync/Heartbeat/Leave/OffsetCommit/OffsetFetch via the
  coordinator (TD-007); group = single owner of the groups shard for `hash(g)`.
- Gate: Java consumer group commits offsets, restarts, and rebalances after a
  broker dies (stateless — any broker serves).

### M5: Transactions / Exactly-Once

- InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/TxnOffsetCommit/EndTxn;
  `end_txn` = one coordinator transaction (TD-008); `read_committed` + LSO +
  aborted-range filtering.
- Gate: differential EOS + Jepsen transactional invariants (TP-003) vs Kafka.

### M6: Compaction, fetch cache, operations

- L0→L1 compaction (must keep up with ingest, D9), locality-aware/per-AZ fetch
  cache, orphan/tombstone GC, metrics, config validation, runbooks, fault harness.
- Gate: compaction-keeps-up + GET/PUT-count invariance (D6/D7/D9) under sustained load.

### M7: Performance and cost proof

- OMB + kafka-perf per ADR-006 profile; cost accounting ($/GB, PUT/GET per MB,
  zero inter-AZ).
- Gate: meets the Phase-4 stop condition (TP-003) — parity + scoped perf + cost/ops.

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
- **Coordinator latency fails the spike (M0)**: this is the load-bearing risk.
  Postgres is expected to pass; if no self-hosted backend meets the produce floor,
  escalate per ADR-008/ADR-004 (different-product re-confirmation) rather than
  patching forward. `CoordinatorStore` pluggability means a backend swap replaces
  a module, not the design.
- Each milestone gate is a test, not a claim; a failed gate rolls the milestone
  back to design (`evolve` the governing TD) instead of patching forward.

## Validation Plan

Every slice gate is defined in TP-002 (increment gates) and, for compatibility
claims, TP-001 (client/tool conformance, performance profiles, fault matrix).
A slice is complete only when its TP-002 gate passes with recorded evidence:
commands, client versions, object-log version, and backend modes.

## Dependency Notes

- **M0 (SPIKE-001) runs first** — the coordinator commit-latency result gates the
  whole produce floor; complete it before committing to M2/M3.
- M1 (gateway on heimq-wire) can start immediately, in parallel with M0.
- M2 (CoordinatorStore + Postgres) is foundational for M3-M5; it can start once
  M0 confirms the backend, in parallel with M1.
- M3 waits for object-log conformance/backend hardening (path/git dependency
  pinned to a recorded SHA; every TP evidence record cites the pinned SHA) AND
  M2 (it calls `commit_object`).
- M4 (groups) and M5 (EOS) build on M2/M3 and are governed by TD-007/TD-008.
- M6 (compaction/cache/ops) runs alongside but its gates (D6/D7/D9) bind before
  the production profile.
- The first build/no-build review (validation checklist, bead `fjord-42864fe0`)
  completes alongside M0/M1.
- M7 (perf/cost proof) only becomes a gate after M3-M6.

## Exit Criteria

- Every supported Kafka API/version appears in the API-001 capability matrix.
- Record data is durable only through object-log/object storage; coordination
  state is in the self-hosted coordinator; no broker-local durable state.
- Standard Kafka clients pass the declared compatibility level (differential vs
  real Kafka/Redpanda on the supported surface, TP-003).
- Build/no-build review still shows meaningful differentiation from
  WarpStream-class systems (self-hosted, no hosted control plane, no consensus).

