---
ddx:
  id: adr-diskless-object-storage-architecture
  depends_on:
    - adr-fjord-as-kafka-compatible-object-log-system
    - adr-client-visible-leader-model
    - adr-durable-metadata-path
    - adr-adopt-heimq-engine-crates
    - td-object-log-data-plane
    - td-metadata-routing-coordination
    - api-kafka-compatibility-surface
    - research-prior-art
    - product-vision
    - prd
---

# ADR-005: Diskless, Sequence-at-Commit Object-Storage Architecture

## Status

**Proposed — contingent on SPIKE-001** (re-baseline, 2026-06-14; amended by
ADR-007 and AR-2026-06-14).

> **Amendment (ADR-007, AR-2026-06-14):** §Decision-2/3's "fully leaderless
> write, offsets assigned by an atomic multi-partition commit" had no
> implementable substrate (object stores lack multi-key atomic commit — AR
> finding B-1). It is replaced by ADR-007's **sharded single-writer sequencer**:
> a Kafka partition's offset sequence is linearized by a single, *dataless,
> instantly-movable* shard owner, not by a contended object-store CAS. "Any node
> accepts any partition" is corrected to "clients route to the shard owner; the
> owner is the real (not presentation-only) sequencing authority." Read this ADR
> for the *diskless data-placement* intent; read **ADR-007** for the authoritative
> sequencing/commit model. This ADR's status is downgraded to contingent because
> its load-bearing bet is unproven until SPIKE-001 runs (AR finding W-e).

Supersedes the data-plane assumptions of TD-003 §Produce Mapping and the
"likely end state" deferral in ADR-003 §Alternatives. Reaffirms and depends on
ADR-004 (metadata stays in object-log internal topics; no hosted control
plane). Does not change the Kafka compatibility surface (API-001).

## Context

fjord's accepted design to date describes a per-partition produce path: a
Produce request resolves to a single owner node (ADR-003 emulated leader),
which appends the batch to that partition's object-log and returns offsets
after commit. The current `fjord-object-log` implementation matches this:
`ObjectLogPartitionLog` assigns offsets from an in-memory `AtomicI64`
(`next_offset`) and writes one object per partition append under exclusive
ownership. That is correct only while exactly one process owns the partition.

Two reference architectures, surveyed in research-prior-art and re-verified for
this decision, show that this per-partition shape is the expensive part of the
design, not the durable substrate:

- **WarpStream** separates *storage from compute* (records go straight to
  object storage; brokers hold no durable state) **and** *data from metadata*
  (ordering/offset assignment lives in a metadata store, not the write path).
  A produce path buffers records from **many topic-partitions and many
  clients** into one in-memory buffer, flushes that buffer as **one object**
  after `~250 ms or ~8 MiB, whichever comes first`, then commits the file's
  metadata; offsets are assigned **at commit time**, so any stateless agent can
  serve any partition with no per-partition leader. One-object-per-partition is
  explicitly called out as economically fatal (~$130/partition/month in PUT
  cost alone). Source: docs.warpstream.com write-path / architecture.
- **Redpanda Cloud Topics** writes payloads directly to object storage on the
  hot path, batching to `~0.25 s or ~4 MB` into an **L0 file**, then
  Raft-replicates only a lightweight *placeholder* (object name + offset) for
  ordering, with a background **Reconciler** compacting L0 into offset-sorted,
  partition-co-located **L1 files**. Source: redpanda.com/blog/cloud-topics-architecture.

Both confirm the same three load-bearing ideas, and both differ from fjord's
current per-partition append model:

1. **Multiplex** many partitions into one object so PUT count is a few per
   second per writer, independent of partition count.
2. **Sequence at metadata-commit**, not at flush — offset assignment is a
   metadata-plane act, which is what removes the per-partition single-writer
   requirement.
3. **Batch interval is the tail-latency/cost dial** — produce latency floor =
   object PUT latency + metadata commit, traded against PUT cost by the flush
   timeout.

fjord's product-vision already commits to "stateless compute over durable
shared storage," "batching amortizes write cost," and names any-node routing as
the end state. What is missing is a decision that makes the **write path
leaderless and multiplexed, and offset assignment a metadata-plane
responsibility** — and that states, as a first-class product property, that
**tail-latency mitigation is the cost-control mechanism**, not a later tuning
pass. ADR-006 specifies that dial; this ADR fixes the architecture it tunes.

The differentiator is unchanged and non-negotiable (product-vision §Critical
Differentiation, ADR-004): the metadata store that WarpStream hosts on
DynamoDB/Spanner, fjord realizes as **object-log internal topics**
(`__fjord_metadata`, `__fjord_groups`) running in the operator's own object
store. fjord is WarpStream's *write/read architecture* without WarpStream's
*hosted control plane*.

## Decision

> **Authoritative model is ADR-008 (2026-06-15); read this §Decision as
> data-placement intent only.** Two supersessions landed on this ADR's sequencing
> mechanism: ADR-007 (single-writer-per-shard) then **ADR-008 (stateless brokers +
> a pluggable central coordinator, default Postgres)** — the latter is current.
> Under ADR-008: **any stateless broker accepts Produce for any partition** (item
> 3 below stands), but ordering/offset assignment is the **coordinator
> transaction** (COORD-001), *not* a metadata-plane commit over object-log
> internal topics (item 4 below is superseded — see item 4 note). "Leaderless" as
> a *write-acceptance* property is true (any broker); as an *architectural-novelty*
> claim it was withdrawn — the coordinator is the real sequencer. The **diskless
> data-placement** intent of this ADR (multiplex partitions → one object, data in
> object storage, compaction, locality-aware fetch) **stands** and is unchanged.

fjord adopts a diskless, stateless-broker, sequence-at-the-coordinator data
plane:

1. **Multiplexed segment write path.** A produce-receiving node buffers record
   batches from many topic-partitions into an in-memory write buffer and
   flushes the buffer as a **single object-log segment object** containing
   batches for multiple `(topic, partition)` tuples. The flush trigger is a
   bounded `(max_delay, max_bytes)` pair (ADR-006 owns the values). One object
   per partition per append is prohibited (already FR-24); this ADR makes
   multiplexing the mechanism, not just a guideline.

2. **Sequencing at metadata-commit.** Offsets are **not** assigned by a
   per-partition in-memory counter on the write path. The durable object is
   written first (durable but not yet "in" any partition), then a
   **metadata-plane commit** assigns the offset range for each
   `(topic, partition)` contained in that object and records the
   object→offset-range mapping. A record is acknowledged only after both the
   object is durable **and** the metadata commit succeeds — equivalent to Kafka
   `acks=all` (consistent with TD-003's acks mapping). The metadata plane is
   the single sequencer per partition; brokers are not.

3. **Leaderless write, presented leadership for reads/routing.** Any node may
   accept Produce for any partition, because ordering is decided at commit, not
   at the writer. Client-visible leadership (ADR-003) is retained **only as a
   routing/cache-locality presentation**: Metadata responses still name an
   owner per partition so standard clients route deterministically and fetch
   caches stay warm, but ownership is no longer a write-correctness boundary.
   This converts ADR-003's "emulated single leader" from a *storage* constraint
   into a *presentation* choice, and pulls its deferred any-node end state into
   the baseline. (TD-002/ADR-003 amendments tracked separately.)

4. **Metadata plane** — *superseded by ADR-008.* This item originally placed the
   sequencer + object→offset index + committed offsets + group state + producer
   idempotency state in object-log internal topics (`__fjord_metadata` /
   `__fjord_groups`). **ADR-008 moves all of it into the pluggable central
   coordinator (default self-hosted Postgres; COORD-001).** The
   no-*hosted*-control-plane principle stands (the coordinator is self-hosted);
   the object-log-internal-topic placement is now an *optional* coordinator
   backend, not the default. The historical reasoning is retained below.
   *(Historical:)* The sequencer, object→offset index, committed offsets, group
   state, and producer idempotency state live in `__fjord_metadata` /
   `__fjord_groups`, replayed into memory on start/takeover. No hosted control
   plane. SPIKE-001's latency
   bars gate this path; ADR-005 raises the stakes on SPIKE-001 because the
   sequencer is now on the produce critical path for *every* partition, not
   just metadata transitions — see Consequences.

5. **Idempotency via metadata, not a write-path leader.** Producer-id/sequence
   de-duplication is enforced at commit by the metadata plane (track last-N
   sequence numbers per `(producer_id, partition)`; drop duplicate batches at
   commit even though already written), the "retroactive tombstone" pattern.
   This preserves Kafka idempotent/transactional semantics without reintroducing
   a per-partition write leader.

6. **Compaction is mandatory.** Small multiplexed ingest objects (L0-equivalent)
   are merged by a background job into larger, offset-sorted, partition-localized
   objects (L1-equivalent) for fetch efficiency and PUT/GET cost. Compaction is
   a required subsystem, not an optional retention feature; reads serve recent
   data via the object→offset index and older data via compacted objects.

7. **Reads via an offset-indexed, locality-aware cache.** Fetch resolves an
   ordered object/offset list from the metadata plane, then reads through a
   node-local (and, when multi-node, consistent-hash/per-AZ) cache keyed on
   object id with aligned chunks, so GET count is decoupled from partition and
   consumer count. (Detailed in a follow-up TD; flagged here as a pillar.)

### Heimq engine implication (cross-repo, flagged)

The current `heimq-broker` `PartitionLog::append` returns assigned offsets
synchronously and the in-memory reference assigns them locally — a shape that
assumes a single writer owns the partition. fjord's sequence-at-commit path
needs the offset-assignment seam to sit **above** the per-partition log, in a
metadata/commit step that can sequence a multiplexed object spanning many
partitions atomically. This is a real `heimq-broker` trait-boundary question
(it is the gap identified when fjord first adopted the engine crates): either
heimq-broker grows a sequencing/commit seam (a `LogBackend`-level atomic
multi-partition append + an offset-assignment hook) that fjord implements, or
fjord owns sequencing entirely above heimq's traits and uses the log traits
only for raw object IO. ADR-005 does not pick the seam; it records that the
seam must be decided with heimq before Phase-3 implementation, and that it must
not force the lossy single-node heimq distribution to carry object-storage or
multi-partition-commit machinery (keeps heimq's charter intact, per the
mode-vs-separate analysis). Tracked as a heimq TRAIT-002 / ADR item.

## Consequences

### Positive

- Cost model becomes WarpStream-grade: PUT count is per-writer-per-second, not
  per-partition; no inter-AZ replication traffic; no local disk. This is the
  concrete realization of the vision's cost-discipline metric.
- Write path is genuinely leaderless, so node add/remove and ownership transfer
  are metadata-only and hot partitions are no longer pinned to one writer's
  throughput.
- Tail-latency mitigation (ADR-006) has a single, well-defined dial (flush
  timeout) with a clear cost relationship, making the vision's "visible and
  configurable latency/cost tradeoff" real.
- Idempotency/transactions stay protocol-correct without a write leader.

### Negative / Risks

- **SPIKE-001 is now higher-stakes.** Sequencing is on the produce critical
  path for every partition. If object-log-internal-topic commit latency cannot
  meet a per-produce commit bar (not just metadata-transition bars), the
  metadata plane needs either a faster durable substrate for the sequencer or a
  batched-commit design (commit the multiplexed object once, sequencing all its
  partitions in one metadata append — which the multiplex design naturally
  enables). SPIKE-001 must be re-scoped to measure per-object commit throughput
  and p99 under multiplexed load. This is the single biggest build/no-build
  risk and must be retired early in Phase 3.
- Ordering correctness now depends on the metadata commit being the lin-point.
  Concurrent writers for the same partition are safe *only* because commit
  serializes them; the commit path must be a correct serialization point
  (property/Jepsen-tested for offset monotonicity, no duplicates, no lost
  writes — Phase 2).
- Compaction and the fetch cache become required earlier than a per-partition
  append model needs them.
- The heimq seam decision is a prerequisite, not a parallel nicety.

## Alternatives Considered

### Keep per-partition append under emulated single leader (status quo)

Rejected as the target. It is correct and simplest for L1 and remains a valid
*interim* during Phase 3 bring-up, but it cannot reach the cost profile (PUT
cost scales with partitions) or the leaderless scaling the vision promises. The
in-memory `AtomicI64` offset assignment is fundamentally single-writer.

### Hosted/Raft metadata sequencer (WarpStream DynamoDB / Redpanda Raft)

Rejected for the core product. A hosted control plane violates the
differentiation gate (ADR-004 §4); a local Raft quorum reintroduces a stateful
consensus system and inter-node coordination that the object-log-internal-topic
path is meant to avoid. fjord's bet is that object-log-as-sequencer can meet the
bars (SPIKE-001). Redpanda's "sub-10 ms local-Raft metadata" is a real
advantage fjord is deliberately trading away for operational simplicity and a
single durable substrate — the vision accepts higher latency for that.

### Async/delayed sequencing (WarpStream "Lightning"-style) from day one

Deferred. Acking before sequencing lowers latency but abandons strict ordering
and cannot support idempotent/transactional producers — incompatible with the
Kafka-parity goal. Revisit only as an explicit opt-in mode after parity is
proven, behind the same surface.
