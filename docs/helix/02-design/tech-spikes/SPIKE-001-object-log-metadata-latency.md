---
ddx:
  id: spike-object-log-metadata-latency
  depends_on:
    - adr-durable-metadata-path
    - td-metadata-routing-coordination
---

# Technical Spike: object-log Internal Topics as Metadata Backend

## Objective

Measure whether coordinator-shaped metadata workloads (offset commits,
metadata transitions, group state replay) over object-log internal topics in
S3-compatible object storage meet the latency bars in ADR-004, before M4
commits to the durable backend implementation.

## Hypothesis

Batched metadata records appended through object-log to an S3-compatible
store can sustain: committed offset commit p99 ≤ 500 ms, metadata transition
(epoch bump, topic create) p99 ≤ 2 s, and coordinator takeover replay ≤ 5 s
for the reference group, without violating FR-24 (no tiny-object writes).

## Approach

- Prototype a minimal internal-topic writer/reader on the current object-log
  API (`ObjectLogBackend` over `LocalObjectStore`, then an S3-compatible
  store — MinIO locally and at least one real S3 region).
- Model three workloads:
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

Pending. Maps to ADR-004: all bars pass → primary path confirmed; offset or
transition bars fail → design the self-hosted Postgres fallback mode; bars
unreachable on any self-hosted path → input to the build/no-build review
(`fjord-42864fe0`).

## Recommendations

Pending.

## Artifacts

- Spike code: throwaway branch in the fjord repo (`spike/metadata-latency`),
  not merged.
- Results: measurement tables and command lines appended to this document;
  decision recorded by updating ADR-004 §Status.
