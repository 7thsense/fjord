---
ddx:
  id: td-transactions-and-exactly-once
  depends_on:
    - adr-pluggable-central-coordinator
    - coord-coordinator-store-contract
    - adr-sharded-single-writer-sequencer
    - td-metadata-plane-state-and-kafka-semantics
    - td-multiplexed-write-path-and-sequencing
    - td-consumer-group-coordinator
    - api-kafka-compatibility-surface
    - ar-rework-re-review-2026-06-14b
    - prd
---

# Technical Design: TD-008 Transactions and Exactly-Once Semantics (EOS)

> **Revised by ADR-008 / COORD-001 (2026-06-15) — resolves AR re-review B1–B4.**
> With a transactional central coordinator (default Postgres), `end_txn` is **one
> coordinator transaction** that atomically: records the txn decision, flips
> pending consumer-offsets → committed, advances participant LSOs / materializes
> markers, and bumps the per-`transactional.id` monotonic `txn_sequence`. This
> dissolves the four blocking gaps the per-shard-marker-fan-out design had:
> - **B1** (offsets+markers atomic across coordinators): one transaction — the
>   `committed_offsets` and txn rows commit together (no `__fjord_groups`-as-
>   participant dance).
> - **B2** (concurrent-txn LSO): `partition_state.lso` computed as `min` over the
>   open-txn set in `txn_state` — a query, not a scalar.
> - **B3/B4** (fencing, marker dedup): `producer_state.epoch` and
>   `txn_state.txn_sequence` under row locks; re-drive is idempotent by sequence.
> - **W** (EndTxn hang): EndTxn returns once the decision row is durable; marker/
>   LSO materialization for the read side proceeds asynchronously and is retried.
>
> The cross-shard two-phase protocol below is **retained only for the optional
> object-log coordinator backend** (which lacks multi-key transactions); for the
> default Postgres/etcd/Dragonfly backends the single-transaction model above is
> authoritative. The Kafka wire lifecycle (InitProducerId/AddPartitionsToTxn/
> AddOffsetsToTxn/EndTxn, read_committed/LSO, fencing semantics) is unchanged.

## Scope

Full design of Kafka transactional/EOS semantics on fjord's single-writer-per-
shard, object-storage model — authored **before** Phase-3 code per the operator
decision (2026-06-15, AR-2026-06-14b N-B5). Covers the transactional producer
lifecycle, the transaction coordinator, **multi-shard commit coordination**,
transaction markers, LSO/`read_committed`, abort handling/GC, fencing, and
recovery. Idempotent-producer semantics (the EOS floor) are in TD-007; this TD
builds on them. Maps directly onto Kafka's transaction protocol so standard
clients work unchanged.

## EOS on a transactional coordinator — the DEFAULT path (ADR-008, build this first)

For the default Postgres coordinator (and any `multi_key_transaction` backend),
EOS does **not** need the cross-shard two-phase marker fan-out described later
(that is retained only for the optional object-log backend). The Kafka wire
lifecycle is unchanged (`InitProducerId`/`AddPartitionsToTxn`/`AddOffsetsToTxn`/
`TxnOffsetCommit`/`EndTxn`); the mapping to COORD-001 rows:

- `InitProducerId(transactional.id)` → upsert `txn_state`, bump `epoch` (fences
  zombies), allocate `producer_id`.
- `AddPartitionsToTxn` → INSERT `txn_partition(transactional_id, topic, partition,
  first_offset)` as each participant's first transactional offset is assigned
  (this is what holds LSO; COORD-001 B1).
- transactional Produce → normal `commit_object`, tagged transactional; offsets
  assigned, but `partition_state.lso` stays pinned at the open txn's min
  first-offset, so `read_committed` cannot see them yet.
- `AddOffsetsToTxn`/`TxnOffsetCommit` → write `committed_offsets` rows with
  `pending_txn = transactional_id` (pending, invisible to `OffsetFetch`).

**`end_txn(commit)` is ONE coordinator transaction** — and the **sync/async
boundary (W2)** is:

*Synchronous (in the transaction; the producer's EndTxn returns only after this
commits):*
1. `txn_state.state = CompleteCommit`, bump `txn_sequence`.
2. **Flip offsets:** `UPDATE committed_offsets SET pending_txn = NULL` for the
   txn — now visible to `OffsetFetch`.
3. **Advance LSO:** `DELETE txn_partition` rows for the txn; recompute each
   participant `partition_state.lso = COALESCE(MIN(remaining open first_offset),
   hw)` — now `read_committed` can see the produced data.

Because the offset-flip (2) and the LSO advance (3) are in the **same**
transaction, the two halves of consume-transform-produce EOS become visible
atomically on the read side — closing the W2 ambiguity. `end_txn` returns to the
producer once this commits; there is no hang on participant acks (no fan-out).

*Asynchronous (after the producer is told success; correctness does not depend on
it):* materializing the **commit/abort control-batch bytes** into the partition's
object stream for clients that parse control records. Read-side visibility is
already governed by `lso` (advanced synchronously) and `aborted_ranges`, so the
physical control batch can lag and be retried; the FetchResponse aborted-list is
served from `aborted_ranges` (COORD-001 W3), not from a materialized marker.

**`end_txn(abort)`** is symmetric in one transaction: `state = CompleteAbort`,
bump sequence, DELETE the txn's pending `committed_offsets`, INSERT
`aborted_ranges(topic, partition, producer_id, first_offset, last_offset)` for
each participant, DELETE `txn_partition`, recompute `lso`. `read_committed` Fetch
filters those ranges (COORD-001 `aborted_transactions` op). Aborted data bytes
remain in object storage and are dropped by compaction (TD-005); the
`aborted_ranges` row is GC'd once `log_start_offset > last_offset`.

**Recovery (default path):** a coordinator crash mid-`end_txn` is resolved by
Postgres transaction atomicity — the transaction either committed or did not;
there is **no partial state and nothing to re-drive** (unlike the object-log 2PC
path below). `transaction.timeout.ms` expiry drives `end_txn(abort)` and bumps
`epoch`, fencing a slow producer's late commit.

The object-log-backend two-phase design below is **retained only** for that
non-transactional backend.

## Design principle (object-log backend only — retained)

Kafka's transaction protocol is already a two-phase commit driven by a
transaction coordinator that fans out markers to participant partitions. fjord
keeps that protocol verbatim on the wire and maps its two roles onto the existing
single-writer-per-shard machinery (ADR-007, TD-007):

- **Transaction coordinator** = the single owner of the `__fjord_txn` shard for
  `hash(transactional.id)` — same dataless, epoch-fenced, replay-on-takeover
  owner as the group coordinator (TD-007 §Coordinator). It owns the durable
  transaction state machine.
- **Participant** = the sequencer-shard owner of each `(topic, partition)` the
  transaction wrote to. Marker writes are single-writer appends to those shards.

The commit **decision** is a single durable append to one shard log (the txn
shard) — atomic by ADR-007's single-writer property. Marker propagation to
participant shards is guaranteed-eventually and re-driven on failover, exactly as
Kafka propagates markers asynchronously. This preserves Kafka's actual visibility
semantics (consumers block at LSO until the marker arrives).

## Transactional producer lifecycle (wire-unchanged)

1. **`InitProducerId(transactional.id)`** → routed to the txn coordinator (via
   `FindCoordinator` type=transaction). Coordinator allocates/loads the
   `producer_id`, **bumps the producer epoch** (fencing any prior incarnation —
   returns `PRODUCER_FENCED`/`INVALID_PRODUCER_EPOCH` to zombies), and persists
   `(transactional.id → producer_id, epoch, state)` to the txn shard log. Aborts
   any in-flight transaction from the prior epoch (recovery, below).
2. **`AddPartitionsToTxn`** → coordinator records the participant `(topic,
   partition)` set in the txn shard log (durably, before data is allowed) so it
   knows whom to send markers to.
3. **Produce (transactional batches)** → routed to each partition's sequencer-
   shard owner as normal (TD-005), tagged with `producer_id`/epoch and the
   transactional flag. The owner sequences them (offsets assigned) but holds
   **LSO** at the transaction's first offset for that partition (TD-007), so
   `read_committed` consumers cannot see them yet.
4. **`AddOffsetsToTxn` + `TxnOffsetCommit`** → consumer-group offsets that are
   part of the transaction are written through the txn coordinator and the group
   coordinator (TD-007), committed atomically with the transaction (the
   consume-transform-produce / EOS-on-offsets path).
5. **`EndTxn(commit|abort)`** → drives the two-phase commit below.

## Two-phase commit (multi-shard coordination — the core of N-B5)

On `EndTxn(commit)`:

1. **Prepare (the decision point).** The coordinator appends a **`PrepareCommit`**
   record (with the participant set and producer_id/epoch) to its `__fjord_txn`
   shard log. This single single-writer append **is** the atomic commit decision
   — once durable, the transaction *will* commit, even across failures. (Abort is
   symmetric: `PrepareAbort`.)
2. **Marker fan-out.** The coordinator sends `WriteTxnMarkers` to each
   **participant shard owner**; each appends a **commit (or abort) control batch**
   to the relevant partition(s) in its shard log (a single-writer append, TD-005
   step 4), which **advances that partition's LSO** past the transaction's
   offsets — making committed data visible to `read_committed` (or marking the
   range aborted). Marker appends are **idempotent** (keyed by producer_id/epoch/
   txn), so re-delivery after failover is safe.
3. **Complete.** When all participant owners ack their markers, the coordinator
   appends **`CompleteCommit`** to the txn shard and replies to the producer.

**Cross-shard atomicity argument.** Atomicity is anchored at step 1: the decision
is one durable single-writer append. Steps 2–3 are guaranteed-eventually — if the
coordinator (or a participant owner) crashes after step 1, recovery re-drives the
markers (they are idempotent). No participant can "partially abort" a committed
transaction because the marker it writes is dictated by the durable decision
record, not by local state. This is precisely Kafka's protocol; fjord changes
only *where* the logs live (object-storage shard logs vs local disk), not the
protocol.

## LSO, visibility, and `read_committed`

- A partition's **LSO** = min over open transactions of their first offset on that
  partition (TD-007). Transactional produce holds LSO; the commit/abort **marker
  append advances it**. `read_committed` fetches are bounded by LSO;
  `read_uncommitted` by HW (TD-007). This gives Kafka's exact visibility timing:
  data is durable and offset-assigned at produce, but invisible to committed
  reads until the marker propagates.
- **Aborted data** stays in immutable object storage; the participant shard
  maintains an **aborted-transaction index** (offset ranges to skip) returned in
  FetchResponse's aborted-transactions list, so `read_committed` clients filter
  them exactly as against Kafka. Compaction (TD-005) **physically drops aborted
  ranges** when building L1, and the abort index is GC'd once no L0 references the
  range (ties to TD-007 orphan/tombstone GC).

## Fencing and zombies

- Producer epoch for a `transactional.id` is owned by the txn coordinator; a
  fenced/zombie producer's transactional produce or `EndTxn` is rejected with
  `INVALID_PRODUCER_EPOCH`/`PRODUCER_FENCED`. Participant owners also carry the
  epoch (from `AddPartitionsToTxn` state) so a zombie's stray transactional batch
  to a participant is rejected at sequencing, not just at the coordinator.
- This composes with TD-007's per-partition idempotent fencing.

## Recovery (coordinator or participant failover mid-transaction)

- **Coordinator failover:** the new `__fjord_txn` shard owner replays the txn
  shard log. State `Ongoing` → leave open (subject to `transaction.timeout.ms`).
  `PrepareCommit`/`PrepareAbort` without `Complete*` → **re-drive marker fan-out**
  (idempotent) then write `Complete*`. This is the standard Kafka recovery; it is
  bounded by the same takeover-replay budget as other shards (ADR-007 / N-B2).
- **Participant owner failover:** the new sequencer-shard owner replays its shard
  log (markers included), so already-written markers and LSO are reconstructed; a
  re-sent marker is idempotent.
- **Hanging transactions / timeout:** the coordinator aborts transactions that
  exceed `transaction.timeout.ms` (drives `PrepareAbort` → markers → CompleteAbort).

## Heimq seam

Transactions live entirely in fjord (txn coordinator state machine, marker fan-
out, LSO/abort index) above the `object_log` `ObjectStore`, consistent with S1
(ADR-007 §5). `heimq-broker` already models transaction-coordinator semantics for
its in-memory single-node case; fjord reuses the wire/handler scaffolding but owns
the durable, sharded, multi-owner coordination heimq's single-node engine does
not provide. No heimq trait change required.

## Tests (feed TP-003 — gate the Phase-4 EOS condition)

- Differential vs real Kafka (O1): commit/abort visibility, aborted-data
  filtering, `read_committed` LSO boundary, exactly-once consume-transform-produce.
- Jepsen (O5) transactional workload: aborted reads (G1a), no duplicates, no lost
  writes, cycle detection — asserting fjord **matches Kafka's** behavior including
  the registered KIP-890 write-cycle artifact (TP-003), not an idealized one.
- DST: coordinator failover between Prepare and Complete re-drives markers exactly
  once (idempotent); participant failover after marker reconstructs LSO; no double
  marker, no lost commit, no visible aborted data.
- Fencing: zombie transactional producer fenced at both coordinator and
  participant; `transaction.timeout.ms` expiry aborts and is visible correctly.
- Multi-shard: a transaction spanning partitions on N distinct shard owners
  commits atomically at the decision point and becomes visible on all N as markers
  land; killing one participant owner mid-fan-out does not lose atomicity.
