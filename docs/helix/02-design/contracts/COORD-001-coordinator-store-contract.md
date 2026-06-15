---
ddx:
  id: coord-coordinator-store-contract
  depends_on:
    - adr-pluggable-central-coordinator
    - td-metadata-plane-state-and-kafka-semantics
    - td-transactions-and-exactly-once
    - td-multiplexed-write-path-and-sequencing
    - api-kafka-compatibility-surface
    - prd
---

# COORD-001: CoordinatorStore Contract

## Scope

The normative interface for the pluggable central coordinator (ADR-008): the
operations brokers call to sequence, coordinate, and persist all non-record
state, the **capability contract** a backend must satisfy, the **atomicity
guarantees** the operations require, and the **Postgres reference schema**. Record
data lives in object storage (TD-005/006) and is out of scope here. This contract
is to the coordinator what heimq's TRAIT-001 is to its log backends: backends are
"supported" only by passing the conformance suite against it.

## Design tenets

- **One serialization point per partition.** All offset assignment for a
  partition goes through the coordinator; brokers never assign offsets. This is
  what makes brokers stateless and any-broker-serves-any-partition correct.
- **Atomic where Kafka needs atomic.** Operations that Kafka requires to be
  all-or-nothing (multi-partition produce commit, EOS commit of offsets + markers
  + txn state) are **single transactions** in the backend. The contract *requires*
  this; backends that cannot provide it are not conformant for those operations.
- **Capability-gated, not silently-degraded.** A backend declares its consistency
  and durability; the coordinator refuses to use a backend for an operation whose
  required guarantee it cannot meet (mirrors heimq capability structs).

## Capability contract

```
CoordinatorCapabilities {
  name: &str,                       // "postgres" | "etcd" | "dragonfly" | "object-log"
  linearizable_writes: bool,        // required = true for sequencing/commit
  multi_key_transaction: bool,      // required = true for EOS commit (offsets+markers+txn)
  durability: Durability,           // None | Async | Sync(fsync/quorum)
  survives_restart: bool,           // required = true
  monotonic_lease: bool,            // required for membership/assignment leases
  approx_write_throughput,          // characterized by the perf suite, advisory
}
```

The coordinator asserts the required capabilities at startup for the configured
backend and refuses to start (clear config error) if unmet — e.g. a Dragonfly
configured without durability fails `durability >= Sync` / `survives_restart`.

## Operations (the trait surface)

Grouped; each notes its **atomicity** requirement. All are `Result<_>`; all
mutating ops are fenced by the relevant epoch where noted.

**Metadata**
- `create_topic` / `delete_topic` / `describe` / `list_topics` — single-key
  transactional; topic config + partition count.
- `partition_assignment(topic, partition) -> BrokerRoute` — the routing/cache-
  locality hint surfaced in Kafka Metadata (no longer a correctness authority,
  ADR-008 §2); `set_assignment` under a membership lease.

**Sequencing (produce critical path)** — *linearizable per partition*
- `commit_object(object_id, [(topic, partition, batch_meta…)]) -> [(partition, base_offset, count)]`
  — the core call: for each partition in the multiplexed L0 object, run the
  idempotency/epoch check, assign a contiguous offset range from the partition's
  current HW, advance HW (and LSO if transactional), append the object→offset
  index entry. **Atomic across all partitions in the object** — one backend
  transaction. Returns assigned base offsets (brokers ack from this).
- `index_lookup(topic, partition, fetch_offset, max_bytes) -> [IndexEntry]` — the
  ordered object/byte-range/offset list for Fetch (TD-006). Read-only,
  read-your-writes consistent.
- `high_watermark` / `log_start_offset` / `last_stable_offset(topic, partition)`.
- `aborted_transactions(topic, partition, fetch_offset, max_bytes) -> [(producer_id, first_offset)]`
  (W3) — the aborted-range list the Fetch path returns in FetchResponse for
  `read_committed` filtering. Read-only.
- `offset_for_leader_epoch(topic, partition, leader_epoch) -> end_offset` (W5) —
  answers the Kafka `OffsetForLeaderEpoch` API from `leader_epoch_history`.
  `leader_epoch` is bumped (and the prior epoch's end_offset recorded) only on a
  **coordinator-driven assignment change**, not on every routing-hint change, so
  it does not churn with load-balancer movement.

**Producer idempotency** — *linearizable; checked inside `commit_object`*
- producer state per `(producer_id, partition)`: current `producer_epoch`,
  last-5 `(base_sequence -> assigned_base_offset)`. `init_producer_id` allocates/
  bumps epoch (fences zombies).

**Consumer groups** — *single-key transactional per group; group is the unit*
- `find_coordinator(group)` (informational — any broker can serve via the
  coordinator), `join/sync/heartbeat/leave` state transitions, `offset_commit` /
  `offset_fetch`. Generation IDs monotonic (enforced by the store, not a broker).

**Transactions / EOS** — *multi-key transactional (REQUIRED)*
- `init_producer_id(transactional.id)`: allocate pid, bump epoch, fence.
- `add_partitions_to_txn`, `add_offsets_to_txn`.
- `end_txn(commit|abort)`: in **one backend transaction**, record the txn decision,
  flip pending consumer-offsets → committed, advance participant LSOs / write
  marker records, and bump the per-`transactional.id` monotonic `txn_sequence`.
  This is the resolution of AR TD-008 B1/B2/B4: atomicity, concurrent-txn LSO, and
  marker dedup are all properties of the single transaction + monotonic sequence.
- Hanging-txn timeout: `abort_expired(now)` (caller passes time — no clock in the
  store contract). EndTxn returns to the producer once the decision row is durable
  (async marker materialization for read-side LSO, per revised TD-008).

**Membership / leases** — *monotonic lease (REQUIRED for multi-broker)*
- `register_broker` / `renew_lease` / `expire`; partition-assignment changes are
  lease-fenced so a partitioned-away broker cannot keep claiming routes.

## Atomicity summary (what each backend MUST provide)

| Operation | Requirement | Postgres | etcd | Dragonfly |
|-----------|-------------|----------|------|-----------|
| `commit_object` (multi-partition) | linearizable, atomic across partitions | txn | txn (cmp/swap, throughput watch) | MULTI/Lua (durability watch) |
| `end_txn` (EOS) | multi-key atomic | txn | txn | MULTI/Lua + durability |
| group state / offsets | per-group atomic, monotonic gen | txn | lease+txn | MULTI |
| leases | monotonic | row+`now` | native lease | native TTL (persistence watch) |

A backend failing a required cell is **non-conformant for that operation** and the
coordinator refuses it.

## Postgres reference schema (default backend)

```sql
-- metadata
topics(topic PK, partitions, config jsonb, epoch)
-- sequencing: one row per partition holds the cursor.
-- B2: create_topic PRE-INSERTS one partition_state row per partition (hw=0,
-- log_start=0, lso=0, leader_epoch=0) in the same transaction, so commit_object
-- is ALWAYS an UPDATE...RETURNING against an existing, lockable row — never a
-- first-write INSERT race.
partition_state(topic, partition, hw bigint, log_start bigint, lso bigint,
                leader_epoch int,                     -- W5: bumped on assignment change
                PRIMARY KEY(topic, partition))
object_index(topic, partition, base_offset bigint, object_id text,
             byte_start int, byte_len int, record_count int,
             PRIMARY KEY(topic, partition, base_offset))
leader_epoch_history(topic, partition, leader_epoch int, end_offset bigint,
                     PRIMARY KEY(topic, partition, leader_epoch))  -- W5: answers OFLRE
-- producer idempotency (last-5 as child rows for safe RMW under the partition lock)
producer_state(producer_id, partition, epoch int, PRIMARY KEY(producer_id, partition))
producer_seq(producer_id, partition, base_sequence bigint, assigned_base_offset bigint,
             PRIMARY KEY(producer_id, partition, base_sequence))  -- last-5 retained
-- consumer groups
group_state(group PK, generation int, leader text, members jsonb, state text)
committed_offsets(group, topic, partition, offset bigint, leader_epoch int,
                  metadata text, pending_txn text NULL,  -- EOS: set while txn open, NULL'd on commit
                  PRIMARY KEY(group, topic, partition))
-- transactions
txn_state(transactional_id PK, producer_id, epoch int, txn_sequence bigint,
          state text, started_at_token bigint)
-- B1: per-(txn, partition) first offset, so LSO = min(open first_offsets) is computable
txn_partition(transactional_id, topic, partition, first_offset bigint,
              PRIMARY KEY(transactional_id, topic, partition))
-- W3: aborted offset ranges for read_committed Fetch filtering
aborted_ranges(topic, partition, producer_id, first_offset bigint, last_offset bigint,
               PRIMARY KEY(topic, partition, first_offset))
-- membership
brokers(broker_id PK, endpoint, lease_until_token bigint, az text)
```

**`commit_object` (per-partition linearizable, atomic across the object) —
mandated lock order (B2, B3):**
```
BEGIN;
  for each (topic, partition) in the object, in a stable order:
    UPDATE partition_state SET hw = hw + n WHERE topic=? AND partition=? RETURNING hw;  -- locks the row FIRST
    -- now, holding that row lock, do the idempotency/epoch check:
    SELECT epoch FROM producer_state WHERE producer_id=? AND partition=?;  -- epoch fence
    SELECT assigned_base_offset FROM producer_seq WHERE ... base_sequence=?;  -- dup?
    -- assign offsets from the returned old hw; INSERT object_index; upsert producer_seq (trim to last-5)
COMMIT;
```
The `partition_state` row `UPDATE` is acquired **before** any `producer_state`/
`producer_seq` read for that partition, so all idempotency/epoch decisions are made
under the per-partition lock — no lost-update interleaving (B3). The row update is
the per-partition serialization point; Postgres MVCC/row locks give per-partition
linearizability with no global lock. Duplicate (matching `producer_seq`) returns
its stored `assigned_base_offset`; gap → OUT_OF_ORDER; stale `epoch` →
INVALID_PRODUCER_EPOCH.

**LSO (B1):** `last_stable_offset` reads `partition_state.lso`, maintained as a
**materialized cache**: on `add_partitions_to_txn`/first transactional write,
INSERT `txn_partition(first_offset)`; on `end_txn`, DELETE that txn's
`txn_partition` rows and **recompute** `partition_state.lso = COALESCE(MIN(first_offset
over remaining open txn_partition rows for the partition), hw)` — all inside the
`end_txn` transaction. So LSO is correct under concurrent transactions on one
partition and is a cheap column read on the Fetch path.

## Conformance suite (feeds TP-003 as an oracle)

A backend is "supported" only if it passes, run against Postgres, etcd, and
Dragonfly (and the optional object-log backend, which is expected to *fail* the
low-latency targets — that is the point of demoting it):
- **Linearizability:** concurrent `commit_object` for the same partition yields
  contiguous, unique, monotonic offsets; no lost/duplicate index entries.
- **EOS atomicity:** `end_txn` is all-or-nothing under injected mid-commit crash;
  offsets + markers + txn state never partially applied.
- **Idempotency/fencing:** duplicate sequence returns prior offset; gap →
  OUT_OF_ORDER; stale epoch → INVALID_PRODUCER_EPOCH.
- **Group monotonicity:** generation IDs never regress under concurrent joins.
- **Lease fencing:** an expired-lease broker's writes are rejected.
- **Durability:** committed state survives backend restart (capability honored).
- **Per-backend perf** (re-points SPIKE-001): `commit_object` and `end_txn`
  p50/p99/throughput per backend; Postgres expected to pass comfortably; etcd
  write-throughput and Dragonfly durability characterized, with pass/fail vs the
  coordinator-latency budget that now sets the produce floor (ADR-006, revised).

## Heimq seam

The coordinator is fjord-owned, above `object_log`'s `ObjectStore` (S1, ADR-007/
008). It is unrelated to heimq's in-memory single-node coordinator; fjord reuses
heimq-wire/handler scaffolding but the durable, pluggable, multi-broker
coordination is fjord's. No heimq trait change.
