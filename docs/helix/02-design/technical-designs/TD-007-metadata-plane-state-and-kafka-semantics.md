---
ddx:
  id: td-metadata-plane-state-and-kafka-semantics
  depends_on:
    - adr-pluggable-central-coordinator
    - coord-coordinator-store-contract
    - adr-sharded-single-writer-sequencer
    - adr-diskless-object-storage-architecture
    - adr-durable-metadata-path
    - td-multiplexed-write-path-and-sequencing
    - td-metadata-routing-coordination
    - td-consumer-group-coordinator
    - api-kafka-compatibility-surface
    - ar-diskless-rebaseline-2026-06-14
    - prd
---

# Technical Design: TD-007 Metadata-Plane State and Kafka Semantics

## Scope

Resolve AR-2026-06-14 findings **B-6** (idempotency/producer-epoch fencing),
**B-7** (high-watermark propagation, LSO, OffsetForLeaderEpoch), and **B-8**
(consumer-group coordinator placement) by specifying the durable state behind the
Kafka semantics and how each is derived from it.

> **Revised for ADR-008 (2026-06-15).** This state — HW, LSO, log-start,
> producer-state (epoch, seq→offset), object→offset index, group state, leader
> epoch — now lives in the **central coordinator** (COORD-001, default Postgres),
> not in a per-shard broker owner. Read "the shard owner maintains X" below as
> "the coordinator holds X (row), and the broker reads/writes it via the
> `CoordinatorStore` call." This makes the semantics *even* simpler than the
> single-writer-per-shard framing: every read-modify-write is a coordinator
> transaction at one serialization point, so there is no concurrent-writer race
> and no broker-side state to replay on failover. The Kafka semantics derived
> below are unchanged; only the home of the state moved (broker → coordinator).

## Per-shard state (owned by the shard's single writer)

A sequencer shard owner holds, in memory (rebuilt by replaying
`__fjord_metadata/{shard}` on takeover) and persisted by appending to that log:

| State | Per | Purpose |
|-------|-----|---------|
| `object_index` | (topic, partition) | ordered `(object_id, byte_range, base_offset, record_count)` — the order authority |
| `high_watermark` (HW) | (topic, partition) | last committed offset + 1; visibility boundary |
| `log_start_offset` | (topic, partition) | retention/compaction floor |
| `last_stable_offset` (LSO) | (topic, partition) | first offset of the oldest open transaction; read_committed boundary |
| `producer_state` | (producer_id, partition) | current `producer_epoch`, and last-5 `(base_sequence → assigned_base_offset)` entries |
| `owner_epoch` | shard | monotonic ownership-fencing token (ADR-007 §2) |
| `partition_leader_epoch` | (topic, partition) | bumped on ownership change; answers OffsetForLeaderEpoch |
| `group_state` | group_id (groups-shard) | coordinator state machine (see §Coordinator) |

Every produce/commit is a single append to the shard log carrying the index
entries, HW advances, producer-state updates, and (if transactional) LSO change —
**atomic because it is one append by one writer** (resolves B-1, reaffirmed here).

## Idempotency and producer-epoch fencing (B-6)

At commit, the shard owner processes each batch in the L0 object **in sequence
order**, per `(producer_id, partition)`:

1. **Epoch check (fencing) first.** If `batch.producer_epoch < producer_state.epoch`
   → reject with **`INVALID_PRODUCER_EPOCH`**; the batch gets no index entry (its
   L0 bytes become a tombstone, GC'd per §Orphan/tombstone GC). If
   `batch.producer_epoch > producer_state.epoch`, adopt the new epoch and reset
   sequence tracking (a new producer incarnation).
2. **Sequence check.** Compare `batch.base_sequence` to the expected next:
   - exact next → assign a contiguous offset range from HW, advance HW, append
     index entry, record `(base_sequence → assigned_base_offset)` in the last-5 map.
   - duplicate (matches one of the last-5) → **drop from partition** (no index
     entry) and **return the stored `assigned_base_offset` from the map** in the
     ProduceResponse (resolves AR-B-1: the seq→offset map is now stored, so the
     duplicate gets the correct original offset).
   - gap / outside window → **`OUT_OF_ORDER_SEQUENCE_NUMBER`**.
3. **Single-writer makes the 5-in-flight window correct (resolves AR-W3/B-6c).**
   Because the shard owner is the sole accepter *and* committer for its partitions
   (ADR-007 §4: clients route to the owner), a producer's in-flight batches for a
   partition all arrive at one node in submission order — the cross-node
   reordering that broke the window under "any node accepts any partition" cannot
   occur. **Producer→owner affinity is provided by Kafka's own leader routing**,
   not a new mechanism.

`InitProducerId` allocates `producer_id`/epoch through the metadata plane; the
allocation is itself a shard append. EOS/transaction-marker handling is detailed
in a follow-up (TD-005 defers EOS), but **epoch fencing is specified here and is
not deferred** (AR-B-3).

## High-watermark, LSO, and visibility (B-7)

- **HW** is maintained in shard state and advanced atomically with each commit.
  Because the owner serves Fetch for its partitions (ADR-007 §4), the Fetch path
  reads HW from the owner's in-memory state — **always current, no staleness, no
  re-read of the metadata log on the read path** (resolves AR-B-5's HW-propagation
  hole). Read-your-writes holds: ack and HW advance are the same append.
- **LSO** is tracked per partition: it trails HW by the oldest open transaction's
  first offset, advancing when that transaction commits/aborts. `read_committed`
  fetches are bounded by LSO; `read_uncommitted` by HW. LSO is now explicit state
  (resolves AR-B-5's "LSO absent" → read_committed impossible).
- **FetchResponse** fields (HW, LSO, log-start, `partition_leader_epoch`) come
  from shard state.

## OffsetForLeaderEpoch and "no truncation" (B-7)

Under the dataless single-writer model there is no divergent-log truncation:
record bytes are immutable in object storage and there is exactly one offset
sequence per partition (the shard log). `partition_leader_epoch` is a monotonic
counter bumped on **ownership change**, not on log divergence.

- `OffsetForLeaderEpoch(topic, partition, epoch)` is answered from shard state:
  return the **end offset of that leader epoch** (the HW at the point the epoch
  was superseded), recorded in shard state at each ownership handoff.
- Because logs never diverge, a client following the OFLRE protocol will never be
  told to truncate below a committed offset; the API returns consistent answers
  and Java consumers do not enter truncation loops (resolves AR-B-4). This is
  recorded as a **deliberate, registered parity property** in TP-003's
  expected-divergence register: "leader-epoch reflects ownership changes, not log
  divergence; truncation never occurs."

## Consumer-group coordinator placement (B-8)

Group state lives in `__fjord_groups`, itself sharded the same way (ADR-007). For
a group `g`, the coordinator is the **single owner of the groups-shard for
`hash(g)`** — exactly one node, fenced by the same `owner_epoch`.

- `FindCoordinator(g)` returns that owner. Clients route Join/Sync/Heartbeat/
  OffsetCommit/OffsetFetch there.
- Because the coordinator is a single fenced writer of the group's state machine,
  generation IDs advance monotonically and two nodes cannot concurrently drive a
  rebalance (resolves AR-B-8 split-brain).
- **Coordinator handoff = groups-shard ownership change**, which bumps the
  owner epoch; in-flight members see `NOT_COORDINATOR` and re-discover, and the
  new owner replays group state from the shard log before serving (the rebalance
  generation bump on handoff is recorded). Failover time = groups-shard takeover
  replay (SPIKE-001 Workload 3 bar).
- `ClusterView::partition_leader` / `find_coordinator` (sync heimq traits) are
  served from the owner's in-memory presented-assignment map, which is shard
  state — no async metadata read on the hot path, avoiding the `block_in_place`
  pattern AR-B-8 flagged. The assignment map is updated only on ownership change.

## Orphan / tombstone GC (ties to AR-B-9, W-4)

Batches rejected at commit (stale epoch, duplicate) leave bytes in immutable L0
objects with no index entry; crashed pre-commit objects are fully orphaned.
Neither is referenced by any shard's `object_index`, so **compaction never sees
them**. A background **reconciliation sweep** (per object-storage prefix) lists
objects and deletes any not referenced by the index after a safety TTL (longer
than the max commit latency), reclaiming them. The compactor, when building L1,
**includes only index-referenced byte ranges**, so tombstoned bytes never reach
L1 (resolves AR-W-4/B-6d). Specified as a required subsystem; tested by
TP-003 (orphan reclaimed, not just hidden).

## Tests (feed TP-003)

- Idempotency: duplicate batch returns the **original** assigned offset (asserts
  the seq→offset map); gap → OUT_OF_ORDER; stale epoch → INVALID_PRODUCER_EPOCH
  (differential vs real Kafka, O1).
- Producer-incarnation: epoch bump resets sequence tracking; old-epoch zombie
  fenced.
- read_committed consumer never reads past LSO; aborted data invisible.
- OffsetForLeaderEpoch after ownership handoff returns the correct epoch-end
  offset; no truncation loop (Java client, O1/O3).
- Coordinator failover: kill groups-shard owner mid-rebalance; new owner replays,
  generation bumps, members re-discover, assignment converges, committed offsets
  preserved (D-suite + O1).
- Tombstone/orphan GC: rejected and crashed-pre-commit objects are reclaimed;
  L1 never contains tombstoned bytes.
