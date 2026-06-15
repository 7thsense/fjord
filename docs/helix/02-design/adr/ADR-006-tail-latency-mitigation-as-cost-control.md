---
ddx:
  id: adr-tail-latency-mitigation-as-cost-control
  depends_on:
    - adr-diskless-object-storage-architecture
    - adr-durable-metadata-path
    - td-object-log-data-plane
    - api-kafka-compatibility-surface
    - research-prior-art
    - product-vision
    - prd
---

# ADR-006: Tail-Latency Mitigation as the Cost-Control Lever

## Status

**Proposed — contingent on SPIKE-001** (2026-06-14; latency floor corrected per
AR-2026-06-14 finding B-3). Depends on ADR-005/ADR-007; owns the flush/commit
dial that ADR-005 §Decision-1 leaves unvalued.

## Context

> **Floor correction (AR-2026-06-14 B-3):** the original floor below treated
> `metadata_commit_latency` as a fast (≈10 ms) write, importing WarpStream's
> numbers. But WarpStream commits to DynamoDB; fjord commits by a **durable
> append to a sequencer-shard log on object storage** (ADR-007) — a second
> object-store-class durable op, not a 10 ms write. fjord's honest floor is:
>
> ```
> produce_ack_latency ≈ object_PUT_latency + durable_shard_append_latency
> ```
>
> i.e. **two** durable object-store-class operations (mitigated by ADR-007 §6
> commit-multiplexing + pipelining on the single shard owner), not one PUT plus a
> cheap hosted-store commit. Consequently the profile p99 figures in the table
> below are **hypotheses pending SPIKE-001 Workload 0**, not committed targets;
> they will be re-derived from measured single-owner shard-append latency. The
> original (WarpStream-derived) formula is retained below only as the historical
> reference it was copied from.

Original (superseded) framing — produce latency floor as first stated:

```
produce_ack_latency ≈ object_PUT_latency + metadata_commit_latency
```

Verified reference points (research-prior-art, primary sources):

- S3 Standard PUT of a few-MiB object: **p99 ~400 ms**; WarpStream default
  produce **p50 ~250 ms / p99 ~400–500 ms**, e2e p99 ~900 ms.
- Zonal/express object tiers (S3 Express One Zone): write **~20 ms**;
  WarpStream low-latency config produce **p50 33 ms / p99 <50 ms**.
- Redpanda Cloud Topics batches `~250 ms or ~4 MB`; keeps metadata on local
  Raft for sub-10 ms metadata ops (a latency advantage fjord trades away for
  one durable substrate and no consensus system — product-vision §Key Tradeoff).

The cost of the system is dominated not by storage but by **request count**:
per-partition objects cost ~$130/partition/month in PUTs; inter-AZ replication
is ~80%+ of a classic Kafka bill. The single knob that moves both latency and
cost is the **flush interval**: flush sooner → lower latency, more PUTs (higher
cost); flush later → higher latency, fewer PUTs (lower cost). Multiplexing
(ADR-005) makes PUT count depend on flush frequency and writer count, **not**
partition count, which is what makes the dial usable.

The product-vision is explicit that fjord "must make that latency/cost tradeoff
visible and configurable rather than hiding it." This ADR makes that a
first-class, specified control surface rather than emergent tuning — i.e.
**tail-latency mitigation is the cost-control mechanism**, and the two are the
same dial viewed from two ends.

## Decision

1. **A single primary dial: the segment flush trigger `(max_delay, max_bytes)`.**
   A node flushes its multiplexed write buffer when either bound is hit.
   Defaults: `max_delay = 250 ms`, `max_bytes = 8 MiB` (WarpStream-aligned;
   `max_bytes` tunable 1–64 MiB). `max_delay` is tunable down to **25 ms** for
   low-latency profiles. This is the cost/latency lever; everything else is
   secondary.

2. **Named deployment profiles** expose the tradeoff as discrete, documented
   operating points rather than raw knobs. **The "produce p99" column is a
   hypothesis pending SPIKE-001 Workload 0** (B-3): it must be re-derived from
   measured `object_PUT + durable_shard_append` latency on the real target store,
   and may prove ~2× higher than these WarpStream-derived placeholders.

   | Profile | `max_delay` | Object tier | Produce p99 (hypothesis, pending SPIKE-001) | Relative PUT cost |
   |---|---|---|---|---|
   | `throughput` (default) | 250 ms | standard object store | ≤ 500 ms | 1× (baseline) |
   | `balanced` | 100 ms | standard | ≤ 300 ms | ~2.5× |
   | `low-latency` | 25 ms | zonal/express tier for ingest | ≤ 150 ms | ~10× + express storage premium |

   Profiles are per-deployment (and, later, per-topic). The cost multiplier is
   disclosed, not hidden, satisfying product-vision's "visible tradeoff."

3. **Tail-latency mitigations, in priority order:**
   a. **Multiplexing** (ADR-005) — the precondition; makes flush frequency the
      cost driver, not partition count.
   b. **Flush-timeout tuning** — the primary dial (above).
   c. **Two-tier object storage** — write ingest objects to a fast/zonal tier
      (express-bucket analog) for low PUT latency, then **compact down** to a
      cheap standard tier (ADR-005 §6), so the express premium applies only to
      short-lived ingest objects, not retained data.

      > **Durability requirement (AR-2026-06-14 B-9).** A single zonal/express
      > bucket is **single-AZ**: losing that AZ before compaction to the regional
      > tier would lose *acknowledged* data, silently breaking the `acks=all`
      > equivalence (ADR-005 §2). Therefore, for any profile using a zonal ingest
      > tier, the L0 PUT is a **quorum write across ≥2 zonal buckets in distinct
      > AZs, acked only after ≥2 succeed** (WarpStream's model: ≥3 directory
      > buckets, ack on ≥2). The produce ack still waits on the durable-quorum PUT
      > **and** the shard commit (ADR-007). A deployment that declines the quorum
      > (single zonal bucket) is a **disclosed weaker-durability mode**, not
      > `acks=all`, and must advertise reduced durability — fjord never silently
      > acks `all` against single-AZ storage. The regional `throughput` profile is
      > already multi-AZ-durable via standard object storage and needs no quorum.
   d. **Concurrent in-flight flushes** — a node keeps multiple PUTs in flight so
      buffer accumulation is not serialized behind one slow PUT; hedged/retried
      PUTs cap p99 against single-object stragglers.
   e. **Metadata-commit batching** — one multiplexed object commits all its
      partitions' offsets in a single metadata-plane append, so per-produce
      commit cost amortizes across many partitions (this is what keeps SPIKE-001
      tractable under ADR-005's higher stakes).

4. **`acks` mapping is unchanged (TD-003):** `acks=all`→durable commit boundary;
   `acks=1`→upgraded to durable by default (disclosed higher latency);
   `acks=0`→may return pre-durability with no committed offsets. fjord never
   silently acks weaker than the profile claims.

5. **Explicit non-goal for now: async/delayed sequencing.** Acking before
   sequencing (WarpStream "Lightning") would beat the PUT-latency floor but
   breaks ordering/idempotency (ADR-005 §Alternatives). Not in the parity path.

## Cost & Latency Targets (parity gate inputs)

> **Baseline scoping (AR-2026-06-14b N-B3, operator decision 2026-06-15).** Each
> claim names its baseline; fjord does **not** claim latency parity with
> WarpStream — its honest floor (`PUT + durable_shard_append`, ~2 object-store
> hops, ADR-006 §Context correction) is structurally above WarpStream's
> `PUT + ~10ms-DynamoDB`, and that is accepted in exchange for the
> no-hosted-control-plane / single-substrate purity. "Equal-or-better
> performance" is scoped to **cost + operational** vs WarpStream-class systems and
> to **latency vs classic local-disk Kafka**, not latency vs WarpStream.

These feed Phase-4 proof (the loop's stop condition). fjord must demonstrate, on
the supported surface, on equivalent hardware:

- **Latency (vs classic Kafka + absolute targets):** produce p99 within the
  SPIKE-001-derived absolute target for each profile (the table p99s are
  hypotheses until then). fjord targets being **competitive with object-storage
  class** and **does not target beating local-disk Kafka small-write latency**
  (vision §Key Tradeoff), nor WarpStream's DynamoDB-backed latency. Latency is a
  *disclosed* tradeoff, not a parity claim.
- **Throughput-at-SLA:** sustained MB/s and records/s at a fixed produce-p99
  bound; target equal-or-better than object-storage-class reference systems
  (the bound is fjord's own SPIKE-001-derived p99, not WarpStream's).
- **Cost (vs WarpStream-class AND classic Kafka — the primary "better" claim):**
  PUT/GET request count per MB ingested/consumed independent of partition count;
  **zero inter-AZ replication traffic; no local persistent disk; no consensus
  system to operate**. Reported as $/GB-ingested and $/GB-month-retained vs a
  classic Kafka baseline, and as operational-surface parity vs WarpStream-class
  (without WarpStream's hosted control plane). **This — cost + operational
  simplicity, not latency — is where "better" must hold.**

## Consequences

### Positive

- The cost story is now a designed control surface with measurable targets, not
  a hope. The same dial serves the latency story.
- Two-tier storage + compaction lets fjord offer a genuinely low-latency profile
  without paying express-tier prices on retained data.
- Metadata-commit batching directly de-risks ADR-005's central SPIKE-001 risk.

### Negative

- More moving parts on the hot path (concurrent flushes, hedging, two tiers) to
  hit tail targets; each needs perf tests (Phase 2) to prove it earns its
  complexity.
- Per-topic profiles (later) interact with multiplexing: mixing a 25 ms-topic
  and a 250 ms-topic in one buffer forces the buffer to the tighter bound or to
  per-profile buffers. Deferred; default is per-deployment profile.
- Two-tier storage adds a compaction-driven data-movement path that must be
  correct under failure (objects in ingest tier must survive until compacted).

## Alternatives Considered

### Fixed latency, no exposed dial

Rejected: contradicts product-vision's explicit "visible and configurable"
mandate and prevents cost-sensitive users from trading latency for money.

### Beat the PUT floor with local-disk WAL buffering

Rejected: reintroduces stateful brokers and the disk/replication cost ADR-005
exists to remove. The floor is accepted as the architecture's defining tradeoff.

### Per-request latency/cost selection by clients

Rejected for now: standard Kafka clients have no field to express it; profiles
are an operator-side concern. Per-topic config is the compatible granularity.
