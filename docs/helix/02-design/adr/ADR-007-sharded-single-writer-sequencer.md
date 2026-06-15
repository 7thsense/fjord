---
ddx:
  id: adr-sharded-single-writer-sequencer
  depends_on:
    - adr-diskless-object-storage-architecture
    - adr-tail-latency-mitigation-as-cost-control
    - td-multiplexed-write-path-and-sequencing
    - ar-diskless-rebaseline-2026-06-14
    - spike-object-log-metadata-latency
    - prd
---

# ADR-007: Sharded Single-Writer Sequencer

## Status

**Largely superseded by ADR-008 (2026-06-15).** ADR-007 introduced a
single-writer-per-shard owner *among the brokers* because there was no fast,
strongly-consistent sequencer — the brokers had to be the serialization point.
ADR-008 adds a **pluggable central coordinator (default Postgres)** that *is* the
sequencer, so **brokers return to stateless** and the per-shard-owner-among-brokers
mechanism is **no longer used**. What ADR-007 got right and ADR-008 keeps: the
sequencing authority **owns no durable record data** (it was the brokers; it is
now the coordinator + object storage), so failover/scale never move data. What
changes: there is no per-shard broker owner, no `NOT_LEADER_OR_FOLLOWER`
write-correctness boundary among brokers, and **no failover-replay
produce-unavailability window (N-B2 dissolved)** — any broker serves any partition
by calling the coordinator. "Sharding" survives only as an *internal* scaling
concern of the coordinator backend (e.g. Postgres partitioning), not a broker
topology. Read this ADR for the historical reasoning that motivated the
coordinator; read **ADR-008 / COORD-001** for the authoritative model.

Original status (historical): Proposed (2026-06-14). Resolves AR-2026-06-14
findings B-1, B-2, B-4, B-5; amends ADR-005 §Decision-2/3 and the ADR-006 latency
floor.

## Context

The adversarial review (AR-2026-06-14) found that ADR-005/TD-005's central
write-path claims have no implementable substrate:

- **B-1:** "the commit is atomic across all partitions in the object" — but
  object-log provides per-partition CAS, not cross-partition atomic append.
- **B-4:** a single `__fjord_metadata` sequencer partition is a global
  bottleneck; sharding it naively breaks B-1's single-append atomicity.
- **B-2:** "any node accepts produce for any partition" + a per-partition
  linearization point means N writers on a hot partition serialize via CAS
  retries, making commit latency unbounded under contention.
- **B-5:** offsets *must* be assigned at a serialization point; this cannot live
  inside heimq's `PartitionLog::append` (which returns offsets synchronously),
  so fjord owns sequencing above the heimq traits regardless.

The unavoidable truth the review surfaced: **a Kafka partition's offset sequence
must be linearized somewhere.** WarpStream linearizes it in DynamoDB (a
per-partition conditional transaction); Redpanda Cloud Topics linearizes it in a
per-partition Raft group. fjord's "fully leaderless write" tried to avoid naming
that point and instead leaned on a non-existent multi-partition atomic-commit
primitive over S3. There is no free lunch: the question is not *whether* to have
a per-partition serialization authority, but *where* it lives and how cheap it is
to move.

The key fjord property to preserve is not "no serialization authority" — it is
**"the serialization authority owns no durable data."** Because record bytes live
in object storage, the authority for a partition can move between nodes with a
metadata-only handoff, with zero data copy. That is the actual differentiator
versus classic Kafka (where the leader owns replicated local disk), and it
survives introducing an explicit authority.

## Decision

fjord sequences through a **sharded, single-writer sequencer**:

1. **Sequencer shards.** The metadata sequencer is partitioned into S shards.
   Each `(topic, partition)` maps to exactly one shard by hash. A shard is an
   ordered metadata log (an object-log internal topic partition,
   `__fjord_metadata/{shard}`) holding the object→offset index and
   high-watermark/LSO state for all partitions in that shard. S scales
   horizontally; S ≪ total partitions, so a shard multiplexes many partitions.

2. **Single writer per shard (the sequencer owner).** At any time exactly one
   fjord node is the **owner** of a shard and is the sole appender to that
   shard's metadata log. Single-writer makes the commit a plain ordered append —
   **no CAS, no cross-writer contention, trivially atomic across all partitions
   in the shard** (resolves B-1, B-2). Ownership is assigned and fenced through
   the metadata plane with a monotonic **owner epoch**; a stale owner's append is
   rejected (fencing). Ownership is metadata-only: an owner holds the shard's
   in-memory sequencer tail (recoverable by replay), never durable record data,
   so handoff is a cheap metadata transfer (preserves FR-28 node replaceability).

3. **L0 objects are per-shard.** A write buffer accumulates record batches for
   the partitions of **one shard** and flushes them as one L0 object; that
   object's multi-partition commit is a **single append to that shard's log**
   (resolves B-1). Multiplexing is now "all hot partitions of a shard into one
   object," still decoupling PUT count from partition count as long as S ≪
   partitions (resolves B-4 without breaking atomicity). Throughput scales by
   adding shards/owners.

4. **The sequencer owner IS the client-visible leader (honest amendment to
   ADR-005 §3).** Kafka clients already route Produce/Fetch to the partition
   leader. fjord presents the shard owner as the leader for all partitions in the
   shard, so clients route produce to the owner — which makes the owner the
   single accepter *and* committer, eliminating cross-node commit contention by
   construction (resolves B-2). This is **not** "presentation only" as ADR-005 §3
   claimed; the owner is the real sequencing authority. The retained fjord
   property: the authority owns no data and moves with a metadata-only handoff.
   A non-owner that receives a misrouted Produce returns `NOT_LEADER_OR_FOLLOWER`
   (standard Kafka retry), exactly as before.

5. **Sequencing lives in fjord, above heimq (resolves B-5, S1 made explicit).**
   The buffer→L0→shard-commit→offset-assignment path is fjord-owned code built
   directly on `object_log`'s `ObjectStore`. fjord does **not** route the produce
   path through heimq's `LogBackend`/`PartitionLog::append` (which would assign
   offsets in the wrong place). heimq's broker traits are used on the read/serve
   side or replaced; the "build on heimq engine crates" claim is narrowed
   accordingly (ADR-004 + TD-005 §Heimq seam updated). fjord still reuses
   `heimq-wire` (framing/codec) and `heimq-broker` handler scaffolding where it
   does not dictate offset assignment.

6. **Commit-multiplexing for latency (feeds B-3 honest floor).** Because one
   owner serializes a whole shard, it batches many partitions' commits into one
   durable metadata append, and pipelines successive appends. The honest produce
   floor is `object_PUT + one_durable_shard_append` (two object-store-class
   durable ops, not WarpStream's PUT+10ms-DynamoDB). ADR-006's targets are
   re-derived from this and marked hypotheses pending SPIKE-001 (finding B-3/W-e).

## Consequences

### Positive

- The commit is a single-writer ordered append: atomic, contention-free,
  trivially correct as the per-partition lin-point. Removes the design's largest
  hole.
- Horizontal scale via shard count; a hot partition is bounded by one shard
  owner's append rate — the same fundamental bound every Kafka-compatible system
  has, with the fjord twist that the owner is dataless and instantly movable.
- Idempotency/epoch checks (B-6) become single-writer operations on the owner —
  far simpler to make correct than under concurrent multi-node commit.
- S1 is now coherent and explicit, ending the "fork in disguise" ambiguity.

### Negative / Risks

- Re-introduces an explicit per-shard authority and a `NOT_LEADER_OR_FOLLOWER`
  routing path — less "purely leaderless" than ADR-005 advertised. This is an
  honesty correction, not a regression: the purity was never implementable.
- Owner failover latency is now on the critical path for a shard's partitions:
  detect failure → reassign owner (fenced by epoch) → replay shard tail → resume.
  This is SPIKE-001 Workload 3 (takeover replay) and must meet its bar; bounded
  by keeping shard tails small via compaction/snapshots.

  > **Failover-unavailability budget (AR-2026-06-14b N-B2).** During this window
  > the shard's partitions are **unavailable for produce** (clients see
  > `NOT_LEADER_OR_FOLLOWER` and retry) — a real regression vs Kafka's
  > sub-second leader failover (a Kafka follower already holds the data; fjord
  > replays from object storage). The budget is the **end-to-end** sum, not just
  > replay: `detect (failure-detector timeout) + reassign+fence (owner-epoch CAS)
  > + replay shard tail + resume`. Target: **detect ≤ Td, reassign+fence ≤ Tf,
  > replay ≤ 5 s (SPIKE-001 W3), end-to-end ≤ Td+Tf+5 s**, with Td/Tf set and
  > measured as explicit SPIKE-001/Phase-4 gates (not folded into the replay
  > number). Replay time is bounded **only if** compaction keeps up (D9); the two
  > bars are coupled. This window is **registered as a disclosed, client-observable
  > parity difference** in TP-003's expected-divergence register: "produce
  > unavailability of up to the failover budget per shard-owner failure;
  > consumers/producers observe `NOT_LEADER_OR_FOLLOWER` and standard retry." The
  > per-shard Raft escalation (below) is the lever if this window is unacceptable.
- Shard count S is a capacity-planning knob (too few → hot-shard bottleneck; too
  many → less multiplexing, more PUTs). Needs guidance + a rebalancing story.
  **v1 decision (N-B-warning):** S is **fixed at cluster creation** and over-
  provisioned for headroom; **owner reassignment across nodes** is supported
  (the dataless handoff) but **online shard split/merge is deferred** to a
  follow-up TD with its own re-sequencing-without-gap design. SPIKE-001 W0b
  quantifies the per-shard ceiling so operators can size S; a hot shard in v1 is
  relieved by moving its owner to a less-loaded node, not by splitting. This
  bounds v1 scope without foreclosing online split later.
- The honest two-durable-hop floor (B-3) may still fail SPIKE-001; the single
  writer + commit-multiplexing is the mitigation, but if the shard append itself
  is hundreds of ms on object storage, the floor is high. SPIKE-001 Workload 0
  now measures single-owner shard-append throughput/latency, not multi-writer CAS.

## Alternatives Considered

### Keep "fully leaderless, multi-partition atomic commit over S3"

Rejected: no substrate (B-1). Object stores do not offer multi-key atomic
commit; emulating it needs a consensus/transaction layer fjord explicitly
avoids.

### Per-partition (not per-shard) single owner

Rejected as the unit: correct but one owner per partition means per-partition
metadata logs and no commit multiplexing — reintroduces the tiny-object/per-
partition-PUT problem (FR-24) at the metadata layer. Sharding is what lets one
append sequence many partitions.

### Raft per shard (Redpanda Cloud Topics model)

Deferred, not rejected. A Raft group per shard gives owner failover without
replay-from-object-store and stronger availability, but adds a consensus system
to operate — against the operational-simplicity gate (ADR-004). fjord bets that
single-writer-with-epoch-fencing + object-log-durable shard log + replay-on-
takeover is simpler and good enough; if takeover replay cannot meet bars,
per-shard Raft (or the Postgres-sequencer fallback) is the escalation, same
ladder as SPIKE-001's conclusions.

### Hosted strongly-consistent sequencer (WarpStream DynamoDB model)

Rejected by the differentiation gate (ADR-004 §4), unchanged.
