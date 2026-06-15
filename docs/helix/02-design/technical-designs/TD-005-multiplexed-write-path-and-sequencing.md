---
ddx:
  id: td-multiplexed-write-path-and-sequencing
  depends_on:
    - adr-pluggable-central-coordinator
    - coord-coordinator-store-contract
    - adr-diskless-object-storage-architecture
    - adr-tail-latency-mitigation-as-cost-control
    - adr-durable-metadata-path
    - td-object-log-data-plane
    - td-metadata-routing-coordination
    - api-kafka-compatibility-surface
    - prd
---

# Technical Design: TD-005 Multiplexed Write Path and Sequencing-at-Commit

## Scope

Specify the leaderless produce path mandated by ADR-005: how a Produce request
becomes a durable, sequenced, acknowledged record when many partitions are
multiplexed into one object and offsets are assigned at metadata-commit. Refines
TD-003 (which assumed per-partition append). Read with ADR-006 for the flush/cost
dial. Does not redesign the metadata-plane interface (TD-002) or the Kafka
surface (API-001).

## Object model

Two object kinds in the object store, both written through object-log:

- **L0 ingest object** (`seg/L0/{node_id}/{seq}`): a single object containing
  Kafka record batches for **multiple `(topic, partition)` tuples**, written by
  one node from one flush of its write buffer. Immutable. Carries an internal
  directory: for each contained partition, the byte range and the producer
  metadata (producer_id, producer_epoch, base sequence, record count) needed for
  idempotency checks at commit. The L0 object's batches are **not yet assigned
  Kafka offsets** when written — base offset is a placeholder patched/Resolved
  at read time from the metadata index (see §Offset representation).
- **L1 compacted object** (`seg/L1/{topic}/{partition}/{base_offset:020}`):
  produced by background compaction (ADR-005 §6), offset-sorted and
  partition-localized for fetch efficiency. Equivalent to today's
  `ObjectLogPartitionLog` segment layout.

The metadata plane (`__fjord_metadata`) holds the **object→offset-range index**:
for each `(topic, partition)`, an ordered list of `(object_id, byte_range,
base_offset, record_count)` entries. This index is the source of truth for
ordering; object write order does not determine partition order.

## Produce path (stateless brokers + central coordinator, ADR-008)

> **Revised for ADR-008 (2026-06-15).** The intermediate single-writer-per-shard
> model is superseded: **brokers are stateless and any broker accepts Produce for
> any partition**; the **central coordinator** (COORD-001, default Postgres) is the
> per-partition serialization point. There is no per-shard broker owner and no
> commit-time CAS — the coordinator transaction is the lin-point.

1. **Accept (any broker).** A client's Produce lands on whatever broker the LB /
   bootstrap routed it to (the Metadata "leader" is a routing/cache-locality hint,
   not a correctness boundary — ADR-008 §2). The broker validates, preserves Kafka
   record-batch bytes verbatim, and appends each `(topic, partition)` batch into
   its in-memory **write buffer**. The response is held (gateway bounded queue,
   TD-003 §Batching) until commit.
2. **Buffer & flush trigger.** The buffer multiplexes batches across many
   partitions until the ADR-006 flush trigger `(max_delay, max_bytes)` fires.
3. **Write L0 object.** The broker serializes the buffer to one L0 object and PUTs
   it (durably; quorum across AZs for a zonal tier, ADR-006 B-9). After the PUT the
   data is durable but "not in" any partition yet.
4. **Sequence/commit (the lin-point) — one coordinator transaction.** The broker
   calls `coordinator.commit_object(object_id, [(topic, partition, batch_meta…)])`
   (COORD-001). In **one backend transaction** the coordinator, for every
   `(topic, partition)` in the object: runs the idempotency/epoch check (TD-007),
   assigns a contiguous offset range from the partition's current HW, advances
   HW/LSO, and writes the object→offset index entry — **atomic across all the
   object's partitions** because it is a single transaction at a single
   serialization point. Concurrent `commit_object` calls from different brokers
   touching the same partition are serialized by the coordinator (Postgres row
   locks / MVCC), yielding contiguous monotonic offsets with no broker
   coordination.
5. **Ack.** On commit success each held response completes with its assigned base
   offset — acknowledged only after step 3 **and** step 4 (`acks=all`).

### Concurrency & ordering correctness

- The coordinator is the single serialization point per partition; brokers do not
  coordinate with each other. Concurrent `commit_object` for the same partition is
  serialized by the coordinator transaction, so offsets are contiguous, unique,
  and monotonic regardless of which broker committed or in what PUT order.
- Atomicity across the object's partitions is the transaction's property (one
  commit, all-or-nothing) — this is what object storage could not provide and the
  coordinator does (resolves AR-B-1).
- Cross-object/cross-PUT completion order is irrelevant: offsets are assigned at
  `commit_object` time (step 4), not at PUT time (step 3).
- This is exactly the invariant set Phase-2 property/Jepsen/DST tests target:
  offset monotonicity, unique-offset-per-value, no lost writes, no duplicates —
  established at the coordinator, with stateless brokers.

### Per-node fan-out

> **Simplified by ADR-008.** With stateless brokers there is no per-shard
> ownership, so a broker multiplexes **all** the partitions it is currently
> receiving into **one** buffer and emits **one L0 object per flush cycle** — PUT
> rate is `flush_rate` per broker, independent of partition and shard count (the
> best case for the cost model and FR-24). Sequencing fan-out across partitions is
> the coordinator's transaction (COORD-001), not extra objects. SPIKE-001 measures
> coordinator `commit_object` latency/throughput, not a shards-per-node axis.

### Idempotency at commit

> **Authoritative: TD-007 §Idempotency + COORD-001 (ADR-008).** Race-freedom comes
> from the **coordinator transaction**, not from any broker-side owner/affinity:
> the idempotency/epoch check runs inside `commit_object` **under the
> `partition_state` row lock** (COORD-001 mandates that lock is taken before the
> `producer_state`/`producer_seq` read), so concurrent `commit_object` calls from
> different stateless brokers for the same partition are serialized — the
> 5-in-flight window is race-free with no producer→broker affinity. The
> `producer_seq` rows give duplicates their original `assigned_base_offset`; a gap
> → `OUT_OF_ORDER_SEQUENCE_NUMBER`; a stale `epoch` → `INVALID_PRODUCER_EPOCH`.
> The sketch below is retained for context; TD-007 + COORD-001 are authoritative.

Per `(producer_id, partition)`, the metadata plane tracks the last 5 committed
base sequences (Kafka's max in-flight). At step 4 it compares the incoming
batch's base sequence:
- exact next → accept, assign offsets;
- duplicate of one of the last 5 → **drop the batch from the partition** (the
  bytes remain in the immutable L0 object but get no index entry — the
  "retroactive tombstone" effect), return the previously assigned offset;
- out-of-order (gap) → `OUT_OF_ORDER_SEQUENCE_NUMBER`.
This preserves Kafka idempotent-producer semantics with no per-partition write
leader. Transactional markers commit through the same path; EOS details deferred
to a TD update.

## Offset representation

L0 batches are written before offsets are known, so the Kafka `base_offset`
field in the stored batch is a placeholder.

> **AR-2026-06-14 W-a:** the previously-listed "Option B" (supply base_offset via
> FetchResponse framing without rewriting batch bytes) is **removed — it is
> protocol-invalid**: `base_offset` lives inside the `RecordBatch` header bytes,
> not in any outer FetchResponse field a standard client honors. The only valid
> approach is to patch the batch bytes.

**Approach (A), patch on materialization:** the base_offset (and batch CRC) are
rewritten from the index. To bound cost (AR-N2), the fetch **cache stores the
already-patched chunk** keyed on `(object_id, chunk, resolved_base_offset)` so
patching/CRC happens once per chunk, not per FetchResponse. CRC-recompute cost is
measured against the fetch-latency target (TD-006) before the cache layout is
finalized.

## Fetch path

1. Resolve `(topic, partition, fetch_offset)` to an ordered list of
   `(object_id, byte_range, base_offset)` from the metadata index — spanning L1
   compacted objects (older) and L0 objects (recent, not yet compacted).
2. Read through the locality-aware cache (ADR-005 §7) keyed on object id with
   aligned chunks; dedupe concurrent GETs for the same chunk.
3. Apply offset representation (§above), validate checksums and offset
   continuity, encode FetchResponse with high watermark / log-start / leader
   epoch from metadata.

## Compaction (L0 → L1)

Background job (scheduled via metadata plane) selects L0 objects whose partitions
have enough committed data, merges per-partition batches into offset-sorted L1
objects, writes them to the cheap tier, updates the index to point reads at L1,
and retires the L0 objects after the index switch. Compaction must be crash-safe:
the index switch is the commit point; L0 objects are deleted only after readers
are pointed at L1. Interacts with object-log retention (ADR-004 §Negative).

## Heimq seam (must be resolved before build — ADR-005 cross-repo flag)

The commit-time, multi-partition, atomic offset assignment does not fit
`heimq-broker::PartitionLog::append` (per-partition, returns offsets
synchronously, in-memory counter). Resolve one of:
- **(S1) fjord owns sequencing above heimq traits:** use heimq log traits only
  for raw object IO; fjord implements buffer/flush/commit/index itself. Smallest
  heimq change; fjord reimplements more.
- **(S2) heimq grows a sequencing seam:** a `LogBackend`-level atomic
  multi-partition commit + offset-assignment hook that fjord implements, kept
  out of the lossy single-node distribution (feature/trait-gated) so heimq's
  charter is intact.
Recommendation: start with **S1** (unblocks fjord, zero heimq coupling risk),
extract to **S2** only if niflheim/pqueue need the same seam. Tracked as heimq
TRAIT-002.

## Tests (feed TP-001 / Phase 2)

- Multiplexed L0 round-trip: produce to N partitions, one L0 object, one commit,
  fetch each partition back in order (Memory + Local object store).
- Concurrent-writer offset monotonicity: two nodes flush overlapping partitions;
  assert contiguous, unique, monotonic offsets (property test).
- Idempotency: duplicate base sequence drops from partition, returns prior
  offset; gap → OUT_OF_ORDER (property test against Jepsen invariant).
- Crash between PUT and commit: L0 object orphaned, no index entry, no ack, no
  lost acknowledged write; replay reconstructs index without the orphan.
- Compaction crash-safety: kill mid-compaction; reads stay correct; no offset
  gap or duplication across the L0→L1 switch.
- Cost assertion: PUT count per MB independent of partition count (fixed-MB
  workload across 1 vs 1000 partitions yields ~equal PUTs).
