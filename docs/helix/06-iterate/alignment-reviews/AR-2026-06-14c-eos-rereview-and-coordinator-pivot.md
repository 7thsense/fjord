---
ddx:
  id: ar-eos-rereview-and-coordinator-pivot-2026-06-15
  depends_on:
    - ar-rework-re-review-2026-06-14b
    - td-transactions-and-exactly-once
    - adr-pluggable-central-coordinator
---

# AR 2026-06-15: TD-008 EOS Re-Review + Central-Coordinator Pivot

## Verdict

Phase-2.8 narrow re-review **did not clear.** Two outcomes, then an operator
directive that reshapes the resolution:

### TD-008 (EOS) — 4 blocking correctness gaps (new design, first review)
- **B1** EOS offset commit spans the txn shard and the group shard (two object-log
  logs, two nodes) with no atomic primitive — asserted atomic, wasn't. (Kafka
  makes `__consumer_offsets` a marker *participant*; TD-008 didn't.)
- **B2** LSO modeled as a scalar ("oldest open txn"); concurrent transactions on
  one partition need the *set* of open-txn first-offsets, `LSO = min(remaining)`.
- **B3** participant-side epoch fencing had no state-propagation path.
- **B4** marker idempotency key (`producer_id/epoch/txn`) undefined — no per-txn
  monotonic id, so failover re-drive can't dedup commits of the same PID/epoch.
- Plus W: EndTxn should return on the decision (async marker fan-out), not block
  on all acks (hang risk); aborted-txn list wire semantics; marker control-batch
  offset/materialization in the patch-on-read model.

### Coherence — residual old-model text (N-B1/N-B4 PARTIAL)
ADR-005 §Decision-3 body + §Consequences ("genuinely leaderless," "hot partitions
not pinned," "concurrent writers safe only because commit serializes"), ADR-003
amendment blockquote, TD-003 superseding sentence, SPIKE-001 Conclusions — all
still assert "leaderless / any-node / atomic multi-partition CAS" normatively.
Plus: TP-003 divergence register missing the N-B2 failover entry; TP-003 known-gaps
stale "TD-005 defers EOS"; D11 outside the Phase-4 stop condition; minor cites.

## Operator directive (2026-06-15) → ADR-008

> "Account for a central coordinator in the design. Pluggable, default Postgres;
> consider etcd or Dragonfly."

This is the root-cause fix. **ADR-008** introduces a pluggable, self-hosted
central coordinator (default Postgres), reverting brokers to stateless
(WarpStream-style). It **resolves the EOS gaps by construction**:
- B1 → offsets + markers + txn-state in **one Postgres transaction** (truly atomic).
- B2 → open-txn set + LSO as rows; `min()` query.
- B3/B4 → epoch + per-`transactional.id` monotonic txn sequence in rows; fencing
  and exactly-once marker application are row-locked transactional updates.
- SPIKE-001 latency gamble → ms-class coordinator commit (re-pointed to measure
  per-backend coordinator latency/throughput).
- N-B2 failover regression → dissolved (stateless brokers; no per-shard replay).

The coherence residuals are now folded into a single cleanup toward the **one**
final model — "stateless brokers + central coordinator" — rather than patching the
intermediate single-writer-per-shard text.

## Required action (ADR-008 cascade — gates Phase 3)
New `CoordinatorStore` TD (trait + capability struct + conformance + Postgres
reference schema). Revise TD-007, TD-008 (fold B1–B4 resolutions; async EndTxn),
ADR-007 (reframe), ADR-004 (superseded → optional backend), and purge old-model
text from ADR-005/ADR-003/TD-003/SPIKE-001. Update product-vision differentiation
(self-hosted pluggable coordinator; drop single-substrate). TP-003: CoordinatorStore
conformance + per-backend perf, divergence-register entry (5), D-enumeration,
known-gaps stub. Then a final coherence re-review before Phase 3.
