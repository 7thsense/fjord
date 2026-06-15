---
ddx:
  id: ar-diskless-rebaseline-2026-06-14
  depends_on:
    - adr-diskless-object-storage-architecture
    - adr-tail-latency-mitigation-as-cost-control
    - td-multiplexed-write-path-and-sequencing
    - td-fetch-read-path-and-cache
    - spike-object-log-metadata-latency
    - tp-verification-strategy-oracles-and-properties
---

# AR 2026-06-14: Adversarial Review of the Diskless Re-Baseline

## Verdict

**BLOCKED for implementation.** Three independent critic harnesses (correctness/
parity lens, performance/viability lens, heimq-seam/testability lens) reviewed
ADR-005/006, TD-005/006, the re-scoped SPIKE-001, and TP-003 as one system. The
set is internally *coherent and honest about its central bet*, but carries
multiple BLOCKING design holes with strong cross-reviewer quorum. Phase 3
(implementation) stays blocked until the B-findings below are resolved in the
specs and a re-review clears them.

The throughline: **fjord borrowed WarpStream/Redpanda's write-path numbers and
mechanisms, but those systems sequence against a strongly-consistent, single-
digit-ms metadata store (DynamoDB/Spanner/local-Raft). fjord's differentiator —
metadata in object-log internal topics on S3 — is precisely the thing that makes
the borrowed numbers and the "atomic multi-partition commit" mechanism
inapplicable.** Most blocking findings are facets of this.

## Quorum rules applied

Any BLOCKING stops work. WARNING raised by ≥2 independent reviewers is promoted
to blocking. Findings without spec/trait evidence discarded. Disagreements shown.

## BLOCKING findings (must resolve before Phase 3)

### B-1 — Multi-partition atomic commit has no substrate (quorum: 2/3, promoted strong)
*Reviewers: parity-B2, seam-F2/F4.* TD-005 claims the commit is "atomic across
all partitions in the object," but object-log provides **per-partition** append/
CAS, not cross-partition atomic append. N independent `put_if_absent` calls on K
partition keys are not atomic; a crash mid-commit leaves some partitions ahead of
others with no specified detection/rollback.
**Resolution:** Specify the commit substrate concretely. Recommended: each L0
object's full multi-partition index commits as a **single append to one
`__fjord_metadata` shard** (that shard's append order is the lin-point), OR a
two-phase pending→committed protocol with a startup roll-forward/back pass.
Define partial-commit recovery. This interacts with B-4 (sharding).

### B-2 — The "amortize across partitions" bet does not remove per-partition serialization (quorum: strong single, parity-adjacent)
*Reviewer: perf-B1.* Multiplexing reduces append *count* but N writer nodes all
accepting produce for hot partition P (ADR-005 §3) still serialize at P's commit.
SPIKE-001 sweeps the wrong axis (K partitions/object) and undersizes the axis
that breaks it (concurrent writers per hot partition). Under contention the floor
becomes `PUT + retries×append-RT`, unbounded — invalidating ADR-006's floor.
**Resolution:** Re-scope SPIKE-001 Workload 0 to make **concurrent-writers-per-
hot-partition** the primary axis (sweep to tens of nodes), report retry count and
tail under contention. Decide whether "presented leader" should also be a soft
**commit-affinity hint** to collapse contention (acknowledge this is a partial
re-introduction of leadership) — or accept and bound the contention.

### B-3 — Latency-floor arithmetic is inconsistent with the chosen substrate (quorum: strong single)
*Reviewer: perf-B3.* Floor is `PUT + commit`, but the commit is itself a durable
object-log append (~another PUT-class op, hundreds of ms on S3), not a 10ms
DynamoDB write. So fjord's real floor is ~2× WarpStream's, yet ADR-006 copies
WarpStream's p99 targets (≤500ms throughput, ≤150ms low-latency).
**Resolution:** Rewrite the floor as `PUT + commit_durable_op`; derive fjord's
*own* targets from `2× object-op + contention`; specify which storage tier the
metadata commit writes per profile; mark ADR-006 profile p99 column as
**hypotheses pending SPIKE-001**, not committed targets.

### B-4 — The metadata sequencer is a single-partition global bottleneck; sharding breaks B-1's atomicity (quorum: strong single, ties to B-1)
*Reviewer: seam-F4.* If every commit appends to one `__fjord_metadata` partition,
max commit throughput = one internal-topic partition's append rate (≈4N
commits/s for N nodes) — the single-writer bottleneck the design set out to
remove. Sharding the sequencer reintroduces cross-shard coordination for one L0
object spanning partitions in different shards, breaking B-1's single-append
atomicity.
**Resolution:** Specify the sharding scheme (e.g. one sequencer shard per topic,
or per partition-hash-range) AND how a multi-partition L0 commit stays atomic
across shards — likely by constraining an L0 object's partitions to one shard
(write per-shard L0 objects), which changes the multiplexing/buffer design.

### B-5 — The heimq "S1" seam is a fork in disguise (quorum: strong single, decisive evidence)
*Reviewer: seam-F1/F10.* `LogBackend::append`/`PartitionLog::append` **return
`(base_offset, record_count)` synchronously** — offset assignment *is* the append
(confirmed in trait sigs + current `lib.rs:128` `next_offset.fetch_add`). There
is no "raw object IO without offset assignment" mode. S1 therefore cannot "use
heimq log traits for IO only"; fjord must bypass `LogBackend`/`PartitionLog`/the
broker handler entirely on the produce path and build on the `object_log` crate's
`ObjectStore` directly — reimplementing buffering, offset assignment, topic
lifecycle, fetch resolution, HWM. "Build on heimq engine crates" collapses to
"use the object_log crate." O6 (heimq-testkit conformance) then tests the
*replaced* per-partition path, giving false coverage.
**Resolution:** State plainly in ADR-005/TD-005: S1 = fjord builds its own
produce-path handler on `object_log`'s `ObjectStore`; heimq broker traits are
used on the **read path only or replaced**. Narrow ADR-004's "heimq engine
crates" claim accordingly. Re-scope O6 to the traits fjord actually implements,
and add a fjord-native conformance suite for the buffer/commit/index interfaces.
*(This vindicates the original mode-vs-separate analysis: the offset-assignment
seam was the crux, and the spec was hand-waving it. Decide S1-explicit vs S2
deliberately, not by default.)*

### B-6 — Idempotency-at-commit is underspecified and likely incorrect under leaderless multi-node (quorum: 3/3)
*Reviewers: parity-B1/B3/W1, perf-W3, seam-F7.* Multiple distinct defects:
- (a) Retroactive tombstone "returns the previously assigned offset" but the
  idempotency record stores only *sequences*, not the `(sequence→committed_base_
  offset)` map needed to build a correct ProduceResponse for a duplicate
  (parity-B1).
- (b) **Producer epoch fencing has no enforcement path**: any node writes
  zombie-epoch data to L0 before any check; the commit must reject with
  `INVALID_PRODUCER_EPOCH` against epoch state in the metadata plane — unspecified
  (parity-B3). Not transactional-only; affects idempotent producers.
- (c) The 5-in-flight window bounds reordering **per connection to one leader**;
  under "any node accepts any partition," one producer's batches spread across
  nodes can commit reordered, yielding spurious `OUT_OF_ORDER_SEQUENCE_NUMBER`
  (perf-W3).
- (d) Compactor behavior for tombstoned L0 bytes is undefined (include → invisible
  data in L1; exclude → re-stitch, losing L1's sequential-read advantage) (seam-F7).
**Resolution:** (a) store `(sequence→committed_base_offset)` per last-5; specify
ProduceResponse construction for duplicates. (b) track producer epoch in metadata,
check before sequence, reject `INVALID_PRODUCER_EPOCH`; do not defer to the EOS TD.
(c) choose **producer→accepting-node affinity** for a session OR reorder-at-commit
within the window; state it; add a D2 variant spreading one producer across nodes.
(d) specify compactor tombstone handling and whether L1 may contain tombstoned bytes.

### B-7 — HW propagation undefined; LSO absent; OffsetForLeaderEpoch under presentation-only leadership undefined (quorum: strong single, parity-critical)
*Reviewer: parity-B4/B5.* (a) No HW propagation/staleness model — fetch reads HW
from where, how fresh? (b) **LSO appears nowhere** → `read_committed` isolation
is impossible (consumers read aborted data or nothing). (c) `OffsetForLeaderEpoch`
and the FetchResponse leader-epoch field have no defined meaning when leadership
is "presentation only"; wrong answers drive Java consumers into truncation loops
/ `LogTruncationException`.
**Resolution:** Specify HW maintained in the metadata plane, updated atomically
with each commit, read during TD-006 index resolution; add **LSO** as tracked
metadata-plane state (open-txn boundary); define "presented leader epoch" as a
metadata counter tied to ownership changes and specify how OFLRE is answered from
it, documenting that Kafka-sense log truncation cannot occur (immutable objects)
as a deliberate, registered parity difference.

### B-8 — Consumer-group coordinator placement / ClusterView under leaderless model unspecified (quorum: 2/3)
*Reviewers: parity-W2, seam-F5.* No mechanism pins a unique coordinator per group;
two nodes acting as coordinator → diverging generation IDs, conflicting SyncGroup,
duplicate/lost assignments. `ClusterView::partition_leader`/`find_coordinator` are
**sync traits** that now must consult durable metadata (async) — the `block_in_place`
pain already visible in `ObjectLogFjordLog`.
**Resolution:** Specify coordinator placement (e.g. coordinator = presented owner
of the `__fjord_groups` partition for `hash(group_id)`); coordinator handoff bumps
the rebalance generation. Specify how `partition_leader`/`find_coordinator` source
the presented assignment and its consistency; make the ClusterView impl a named
Phase-3 deliverable.

### B-9 — Two-tier express ingest tier can violate acks=all durability (quorum: 2/3)
*Reviewers: seam-F8, perf-W4 (orphan facet), parity-W5 (stranded facet).* S3
Express One Zone is single-AZ; an AZ loss before compaction to the standard tier
loses *acknowledged* data — breaking the `acks=all` equivalence (ADR-005 §2,
ADR-006 §4) for the `low-latency` profile. Separately, orphan/tombstoned L0
objects on the expensive tier have **no GC**, leaking cost monotonically.
**Resolution:** For any zonal ingest tier, specify durability: either quorum-write
across ≥2-3 zonal buckets before ack (WarpStream's model) or **explicitly disclose
weaker durability** for that profile (and stop claiming acks=all). Add an orphan-GC
sweep (list-vs-index reconciliation or ingest-tier TTL); test that orphans are
*reclaimed*, not just excluded from reads.

### B-10 — Phase-4 "parity" stop condition is unfalsifiable as written (quorum: strong single, gates the loop)
*Reviewer: seam-F3.* The expected-divergence register and "declared supported
surface" are both under fjord's unilateral control with no cap, arbiter, or
ineligibility criteria — so "zero unexplained diffs" can always be satisfied by
registering the diff. `acks=1`-upgrade and `__consumer_offsets`-surfacing are
semantic, client-observable differences, not timing artifacts.
**Resolution:** Define categorically-ineligible divergences (anything a *standard
client observes on the produce/fetch/commit surface without config changes*);
freeze the supported surface in API-001 (not adjustable per-run); require external
sign-off + reproducer for register additions; cap/track the register as a
machine-readable artifact with a CI check.

## Promoted/standalone WARNINGS to fix in the same rework

- **W-a (2/3): Option B offset representation is protocol-invalid** (parity-W3,
  seam-F9). `base_offset` lives in the RecordBatch bytes; it cannot be overridden
  via FetchResponse framing. **Drop Option B** (or correct it to "patch bytes");
  keep Option A; measure CRC cost; state whether the cache stores pre-patched bytes.
- **W-b: Compaction throughput vs ingest unbounded → L0 pile-up** (perf-W1).
  Breaks GET-invariance, inflates express cost, blows the 5s replay bar. Add a
  compaction-keeps-up invariant + backpressure policy + L0-fan-out test axis.
- **W-c: CAS/put-if-absent primitive asserted but unspecified / not uniform across
  stores** (perf-B2, seam-F6). Name the primitive + required semantics + minimum
  store capability; run SPIKE-001 on the **real target store, not MinIO** (hard
  gate, not a footnote).
- **W-d: DST mock + in-memory model written by the same team share assumptions →
  false confidence; DST can't be both "unsound pending SPIKE-001" and a merge
  gate** (seam-F6, parity-N1). Derive the mock from object-log CONTRACT-002,
  validate against a real store before relying on DST as a gate; scope DST gating.
- **W-e: Process inversion — ADR-005/006 read as committed Decisions while the
  load-bearing SPIKE-001 is unrun** (perf-W5). Downgrade ADR-005/006 status to
  **"Proposed — contingent on SPIKE-001"**; mark spike-dependent TD sections
  provisional.

## NITs (fix opportunistically)
- D7/cost tests must count **all** PUT-class ops (L0 + metadata commit + L1), not
  just data PUTs (perf-N1).
- `ObjectLogOffsetStore::commit` delete-then-put-if-absent has a TOCTOU race
  (seam-F12) — pre-existing code, fix during Phase 3 offset-store rework.
- Per-topic-profile vs multiplexing tension should be a tracked parking-lot
  decision, not buried in ADR-006 Consequences (parity-N4, seam-F13).
- `fjord-42864fe0` build/no-build reference is unresolved; give explicit criteria
  (perf/seam-F11).

## Disagreements / non-quorum (recorded, not blocking)
- Reviewers split on whether the metadata sequencer single-partition design is
  *intended* (seam read it as the likely correct lin-point; perf read it as an
  un-acknowledged bottleneck). Both agree it must be **stated explicitly** — that
  agreement is the actionable part (B-1/B-4).

## Required action

Rework ADR-005/006, TD-005/006, SPIKE-001, and TP-003 to resolve B-1..B-10 and
W-a..W-e, then re-review the changed sections only. Do not begin Phase 3 until the
re-review clears the blocking set.
