---
ddx:
  id: ar-rework-re-review-2026-06-14b
  depends_on:
    - ar-diskless-rebaseline-2026-06-14
    - adr-sharded-single-writer-sequencer
    - td-metadata-plane-state-and-kafka-semantics
---

# AR 2026-06-14b: Re-Review of the Rework

## Verdict

**B-1..B-10 and W-a..W-e are genuinely closed** (resolving text cited per
finding by both reviewers). The ADR-007 single-writer pivot is sound and is "the
honest answer the original leaderless framing was avoiding." **Not yet
implementation-ready:** the rework was applied unevenly and the pivot surfaced
new issues, mostly *honesty/coherence* rather than fresh design holes. Five new
blocking findings (two reviewers, strong convergence):

## New blocking findings

### N-B1 — TD-005 body still describes the old leaderless multi-writer model
*(both reviewers).* Only TD-005's *idempotency* subsection got a superseding
note; §Produce path ("Any node accepts Produce for any partition") and
§Concurrency ("two nodes may flush the same partition concurrently… CAS") still
contradict ADR-007's single-writer model. ADR-005 §Decision-3 body likewise still
says "leaderless." An implementer reading TD-005 builds the B-1/B-2 hole back in.
**Fix:** rewrite TD-005 §Produce path + §Concurrency to single-writer; reconcile
ADR-005 body (move superseded framing to an appendix).

### N-B2 — Failover unavailability window is unbounded and undisclosed
*(reviewer 1).* Single-writer means a shard owner's death makes its partitions
**unavailable for produce** until detect→reassign→fence→replay. Only "≤5s replay"
is cited — covers replay only, not the full window, isn't derived, and regresses
vs Kafka's sub-second leader failover. **Fix:** state an explicit end-to-end
produce-unavailability budget as a SPIKE-001 gate; register the Kafka-failover
regression as a disclosed, client-observable parity difference; couple the replay
bound to D9 (compaction-keeps-up) — meaningless if backlog is unbounded.

### N-B3 — "Equal-or-better performance" is unprovable without a named baseline
*(reviewer 2).* The honest floor (`PUT + durable_shard_append`, two object-store
hops) is structurally ~2× WarpStream's (`PUT + ~10ms DynamoDB`). fjord **cannot**
beat WarpStream on latency — that's physics. The spec conflates three comparisons
(cost vs classic Kafka, latency vs WarpStream, throughput vs unnamed). **Fix:**
scope the Phase-4 performance bar to a *named* baseline per claim: cost/ops parity-
or-better vs WarpStream-class; latency better-than-classic-Kafka and within
object-storage class; absolute p99 targets from SPIKE-001 — and stop implying
latency parity with WarpStream. **This is the user's strategic call (see below).**

### N-B4 — "Leaderless" architectural-novelty claim is no longer true
*(reviewer 2).* Post-pivot, fjord ≈ "Redpanda Cloud Topics without Raft": shard
owner = partition leader, dataless and movable. The remaining differentiation is
real (no consensus system, single durable substrate, open/self-hostable) but
"leaderless" must be retracted. **Fix:** rename to "dataless movable owner";
update product-vision differentiation to an honest comparison vs Redpanda Cloud
Topics specifically (give up Raft availability + sub-10ms metadata to remove the
consensus system — a real, defensible trade).

### N-B5 — "FULLY Kafka-capable" lacks an EOS design and a falsifiable exclusion register
*(reviewer 2).* EOS/transactions are deferred (but `enable.idempotence=true` is
default in modern clients); follower fetch (KIP-392) is structurally excluded;
KIP-932 queues and tiered-storage (KIP-405) protocol interaction unaddressed. No
machine-readable capability boundary. **Fix:** add a Kafka feature-compatibility
matrix to API-001 (Accept / Defer-with-date / Structurally-excluded);
structurally-excluded items go in the divergence register; EOS gets a real
milestone, not an undated "follow-up TD."

## Warnings (fold into the same rework)
- Hot-partition commits/s ceiling is lower than DynamoDB-backed systems by the
  append-latency ratio — state honestly, don't flatten to "same bound."
- Shard rebalancing/split mechanism named-as-needed but unspecified — at least
  state whether S is fixed-at-create for v1.
- Single-node multi-shard produce: a node owning K shards emits ≥K L0 objects per
  flush — add shards-per-node axis to SPIKE-001 W0; confirm PUT rate stays in the
  cost model.
- Simplicity claim: ~11 required subsystems; rephrase to "simpler operator
  responsibility surface / fewer stateful failure modes," not "simpler system."
- Add a high-frequency-ownership-churn consumer test (OFLRE epoch burn).
- Postgres fallback is a *different product* (relational control plane) — add an
  explicit claim-rollback decision tree + hard SPIKE-001 deadline to ADR-004.

## NITs
- TP-003 O6 scope not narrowed to traits fjord actually implements (B-5 residue).
- Phase-4/DST enumerate "D1–D8"; table now has D1–D10.
- SPIKE-001 re-scope banner still calls it "Workload 4"; Approach renumbered to 0.

## Operator decisions (2026-06-15) — binding for the round-2 rework

1. **Performance bar (resolves N-B3, frames N-B4):** success is **cost + operational
   parity-or-better vs WarpStream-class systems**, **latency better than classic
   local-disk Kafka** and within object-storage class — fjord **does not chase
   WarpStream's latency** (the ~2× floor is accepted, it's physics). The
   no-hosted-control-plane / single-durable-substrate purity is **kept** (no
   default Raft, no hosted metadata). Phase-4 perf stop condition rewrites to
   named baselines accordingly; absolute p99 targets come from SPIKE-001.
2. **Kafka capability (resolves N-B5):** **full EOS/transactions is designed
   before any Phase-3 code** — multi-shard commit coordination, transaction
   markers, abort-index GC, coordinator epochs. Idempotent-producer parity
   (TD-007) is the floor; a dedicated EOS TD is now spec work, not a follow-up.
   Structurally-excluded features (follower fetch KIP-392, queues KIP-932,
   tiered-storage KIP-405 protocol) go in API-001's capability matrix +
   divergence register.

## Required action (round-2 rework, now unblocked by the decisions)
N-B1 (rewrite TD-005 produce-path/concurrency to single-writer; reconcile ADR-005
body) · N-B2 (bound + disclose failover-unavailability budget) · N-B3 (re-scope
Phase-4 perf bar per decision 1; fix ADR-006 targets/TP-003) · N-B4 (retract
"leaderless" → "dataless movable owner"; honest vs-Redpanda-Cloud-Topics
positioning in product-vision) · N-B5 (API-001 capability matrix + a new EOS TD
per decision 2) · warnings (shard split/rebalance note, shards-per-node SPIKE
axis, simplicity rephrase, OFLRE-churn test, ADR-004 fallback decision tree +
deadline, O6 scope, D1–D10 enumeration, SPIKE banner). Then a final coherence
pass and a narrow re-review of changed sections before Phase 3.
