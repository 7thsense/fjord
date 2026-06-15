---
ddx:
  id: spike-object-log-metadata-latency
  depends_on:
    - adr-durable-metadata-path
    - adr-diskless-object-storage-architecture
    - td-metadata-routing-coordination
    - td-multiplexed-write-path-and-sequencing
---

# Technical Spike: object-log Internal Topics as Metadata Backend

> **Re-scoped 2026-06-14 for ADR-005.** ADR-005 moves offset *assignment*
> (sequencing) onto the per-produce critical path: every multiplexed L0 object
> is acknowledged only after a metadata-plane **commit** (TD-005 §Produce-path
> step 4) appends to `__fjord_metadata`. The commit is now the dominant,
> highest-frequency metadata workload — not the occasional epoch bump this spike
> originally centered on. **Workload 0** below is added and is now the
> **primary** pass/fail gate; if it fails, the sequencer needs a faster substrate
> or a batched-commit redesign. This makes SPIKE-001 the single biggest
> build/no-build risk and it must be retired first in Phase 3. (Under **ADR-007**
> the commit is a **single-writer shard append**, not multi-writer CAS; Workload 0
> is re-scoped to measure single-owner shard-append latency + handoff fencing.)

## Objective

Measure whether (a) the **per-object sequencing commit** and (b) coordinator-
shaped metadata workloads (offset commits, metadata transitions, group state
replay) over object-log internal topics in S3-compatible object storage meet
the latency bars in ADR-004/ADR-005, before Phase 3 commits to the durable
backend implementation.

## Hypothesis

Batched metadata records appended through object-log to an S3-compatible store
can sustain: **sequencing-commit p99 ≤ object_PUT_p99 (i.e. commit does not
dominate the produce floor) at the target multiplexed object rate (a few
objects/s/node)**; committed offset commit p99 ≤ 500 ms; metadata transition
(epoch bump, topic create) p99 ≤ 2 s; and coordinator takeover replay ≤ 5 s for
the reference group, without violating FR-24 (no tiny-object writes). The key
bet: because one commit sequences **all** partitions in a multiplexed object
(TD-005), per-produce commit amortizes and the commit append rate stays at the
object-flush rate, not the per-partition produce rate.

## Approach

- Prototype a minimal internal-topic writer/reader on the current object-log
  API (`ObjectLogBackend` over `LocalObjectStore`, then an S3-compatible
  store — MinIO locally and at least one real S3 region).
- Model four workloads:
  0. **Sequencing commit (PRIMARY, ADR-007 single-writer-per-shard)**: model the
     commit as ADR-007 specifies — a **single shard owner is the sole appender**
     to `__fjord_metadata/{shard}`; there is no multi-writer CAS contention on
     the shard log (that hole is closed by ADR-007). Measure, in priority order:
     - **(0a) Single-owner shard-append latency/throughput** — the produce-
       critical-path durable op. One owner commits multiplexed L0 objects (sweep
       partitions-per-shard = 1, 10, 100, 1000) as single ordered appends, with
       commit-multiplexing/pipelining (ADR-007 §6). Report append p50/p95/p99 and
       max sustained commits/s/owner. **This sets the honest produce floor
       `object_PUT + durable_shard_append` (ADR-006 B-3); its p99 must not dwarf
       the object PUT.** Sweep flush intervals matching all three ADR-006 profiles
       (250 ms / 100 ms / 25 ms), recording pass/fail per profile (AR-W6).
     - **(0b) Hot-shard ceiling** — max produce throughput for partitions pinned
       to one owner (the fundamental per-shard bound); confirms sharding is the
       scaling lever and quantifies when a shard must split.
     - **(0c) Ownership handoff / fencing** — owner-epoch CAS on handoff: latency
       to fence the old owner + install the new one, and that a stale owner's
       append is rejected. This is the only remaining CAS (AR-W-c); measure on the
       **real target store, not MinIO** (AR-W-c hard gate).
  1. **Offset commits**: 50 groups × 20 partitions committing every 5 s,
     measuring commit-acknowledged latency under object-log batching.
  2. **Metadata transitions**: serialized epoch bumps and topic creates with
     CAS contention from 2 concurrent writers.
  3. **Takeover replay**: rebuild in-memory group/offset state from the
     internal topic at three history sizes (1k / 50k / 500k records, with
     and without a compacted snapshot).
- Sweep object-log batching thresholds (commit delay 50 ms – 1 s) to chart
  the latency/cost frontier; record object PUT/GET counts per workload.
- Out of scope: real Kafka protocol handling, multi-node membership, security.

## Findings

Pending — spike not yet run.

### Measurements

Pending. Record per ADR-004 bars: p50/p95/p99 per workload, PUT/GET counts,
object sizes, batching config, store/region, object-log commit SHA.

## Analysis

Pending.

### Risks

- MinIO latency flatters S3; real-region runs are required before a pass.
- Replay bars depend on snapshot/compaction design that object-log M2 has not
  shipped; the spike may need a throwaway snapshot to test the bound.
- CAS contention behavior may differ across S3-compatible stores
  (capability-dependent; see object-log CONTRACT-002).

## Conclusions

> **Re-scoped by ADR-008 (2026-06-15).** The central question is no longer "can
> object-log internal topics carry the sequencer?" — ADR-008 makes a pluggable
> central coordinator (**default Postgres**) the sequencer, so this spike now
> **characterizes `commit_object`/`end_txn` latency + throughput per
> `CoordinatorStore` backend** (COORD-001 conformance/perf). Workloads 0–3 run
> against each backend.

Pending. Maps to ADR-008 / COORD-001:
- **Postgres (default) meets the coordinator-latency budget** → confirmed;
  proceed. (Expected: ms-class commits clear comfortably.)
- **etcd / Dragonfly** → characterize write-throughput (etcd) and durability-mode
  latency (Dragonfly); declare each supported only where it meets the
  `CoordinatorStore` capability + latency contract.
- **object-log internal-topic backend** → expected to *miss* the low-latency
  budget; this measurement quantifies *why* it is the optional, not default,
  backend (purist single-substrate deployments accept the higher floor).
- **No self-hosted backend meets the produce floor at all** → build/no-build
  review. (Far less likely now that Postgres is the default, vs the prior
  object-log-only bet.)

## Recommendations

Pending.

## Artifacts

- Spike code: throwaway branch in the fjord repo (`spike/metadata-latency`),
  not merged.
- Results: measurement tables and command lines appended to this document;
  decision recorded by updating ADR-004 §Status.
