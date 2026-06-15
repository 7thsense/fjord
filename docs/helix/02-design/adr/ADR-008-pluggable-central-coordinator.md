---
ddx:
  id: adr-pluggable-central-coordinator
  depends_on:
    - adr-diskless-object-storage-architecture
    - adr-sharded-single-writer-sequencer
    - adr-durable-metadata-path
    - td-metadata-plane-state-and-kafka-semantics
    - td-transactions-and-exactly-once
    - ar-rework-re-review-2026-06-14b
    - prd
---

# ADR-008: Pluggable Central Coordinator (default Postgres)

## Status

Proposed (operator directive, 2026-06-15). **Supersedes ADR-004's primary path**
(object-log internal topics as the durable metadata/sequencing substrate) and
**reframes ADR-007** (the single-writer-per-shard sequencer was a workaround for
*not* having a fast strongly-consistent coordinator; with one, it is unnecessary).
Retires the central SPIKE-001 bet. Data-placement (ADR-005), the multiplexed
object write path (TD-005), and the fetch/cache path (TD-006) are unchanged.

## Context

The Phase-2.8 re-review (AR-2026-06-14b re-review) found that fjord's hardest
problems all trace to one root cause: **trying to do strongly-consistent
sequencing and coordination over object-log internal topics on object storage**.

- **EOS atomicity (TD-008 B1):** committing consumer offsets atomically with a
  transaction spans two object-log shards on two nodes — object storage has no
  multi-key atomic commit, so it could not be made truly atomic.
- **Concurrent-transaction LSO (B2), fencing (B3), marker idempotency (B4):** all
  need fast, transactional read-modify-write on small coordination state — the
  opposite of what object storage is good at.
- **SPIKE-001 latency risk:** putting a durable object-store append on the
  per-produce critical path gives a ~2× WarpStream floor and a build/no-build
  gamble.
- **Failover regression (ADR-007 N-B2):** the single-writer-per-shard owner made
  partitions unavailable during takeover-replay — a workaround for the missing
  fast sequencer.

Every reference architecture solves this with a fast, strongly-consistent
metadata store on the critical path and **keeps the brokers stateless**:
WarpStream → DynamoDB/Spanner; Redpanda Cloud Topics → per-partition Raft. fjord's
only real differentiator was never "no coordinator" — it was **"no *hosted*
coordinator."** A self-hosted, pluggable coordinator preserves that while
removing all of the above.

## Decision

1. **Introduce a pluggable central Coordinator** — the metadata + sequencing +
   coordination plane, behind a `CoordinatorStore` trait. It owns: topic/partition
   metadata; **offset sequencing** (atomic multi-partition offset assignment +
   HW/LSO advance + object→offset index write); producer idempotency state
   (epoch, seq→offset); consumer-group coordination + committed offsets;
   transaction state + **atomic EOS commit**; and node membership / partition
   assignment (broker → partition routing). Record **data** stays in object
   storage; the coordinator holds only coordination state.

2. **Brokers are stateless again (WarpStream-style).** Any broker accepts Produce
   for any partition, buffers a multiplexed L0 object (TD-005), PUTs it to object
   storage, then calls the coordinator to **sequence/commit** (assign offsets,
   advance HW, write index) in one transactional call. The coordinator is the
   single serialization point per partition — **not** a per-shard broker owner.
   This **removes the failover-unavailability regression (N-B2):** a broker death
   loses no durable state and any broker serves any partition; the coordinator's
   own HA is the store's well-trodden HA. The presented "leader" in Metadata
   becomes a *routing/cache-locality* hint again (load balancing), not a
   correctness authority.

2a. **Deployment topology — the coordinator IS the store, not a fjord daemon.**
   `CoordinatorStore` (COORD-001) is a **client-side library embedded in every
   broker**, not a network service fjord ships. A broker reaches the coordinator
   by a **connection string** (Postgres DSN / etcd endpoints / Dragonfly URL).
   Therefore the operational surface is exactly **two things**: (1) one class of
   fjord server — the **stateless broker** (identical, interchangeable, behind a
   load balancer); (2) one **backing store** the operator already knows how to run
   (Postgres by default). There is **no separate coordinator process** to deploy,
   and **no fjord-specific cluster discovery**: brokers never talk peer-to-peer —
   each talks only to object storage (a bucket URL) and the coordinator (a
   connection string). Broker membership is rows *in* the coordinator, read back
   to populate Metadata; Kafka clients reach brokers via the normal bootstrap/LB
   path. The store's own clustering/discovery (a DSN, pgbouncer, Patroni/RDS,
   etcd quorum) is the only HA to configure, and it is not fjord-specific.
   **Single-node / dev:** an embedded `CoordinatorStore` backend (SQLite or
   in-process) means one process and nothing extra to run — the same "just run the
   binary" story as heimq. A connection pooler or optional coordinator-cache
   sidecar is a *later, optional* scaling aid (high broker counts → DB connection
   fan-in), never required and never a new server class in the base design.

3. **Default backend: self-hosted Postgres.** Transactional → the atomic
   multi-key commit that object storage lacked, resolving EOS B1 (offsets +
   markers + txn state in one transaction), B2 (open-txn set + LSO in rows), B3/B4
   (epoch + per-txn dedup state under row locks). Single-digit-ms commits remove
   the SPIKE-001 latency gamble. Postgres is ubiquitous, self-hostable, and
   operationally well-understood.

4. **Pluggable alternatives behind the trait,** each with a conformance suite
   (mirroring heimq-testkit's per-trait suites):
   - **etcd** — strongly-consistent KV + leases; strong for metadata, membership,
     ownership leases; sequencing *write throughput* is the watch-item (revision
     churn) — validated by the conformance + perf suite before it is a supported
     sequencing backend.
   - **Dragonfly** (Redis-compatible) — very high throughput/low latency; its
     **durability/HA configuration is the watch-item** (in-memory by default), so
     it is supported only in a configuration that meets the coordinator durability
     contract (the trait declares required durability, like heimq's capability
     structs).
   - **object-log internal topics** — the prior ADR-004 path is **retained as an
     optional backend** for the single-substrate purist, explicitly carrying the
     latency/atomicity caveats SPIKE-001 measures; no longer the default.

5. **Self-hosted only — no mandatory hosted/managed service.** Postgres/etcd/
   Dragonfly run in the operator's environment. This preserves the core
   differentiation (open/self-hostable, no SaaS control-plane dependency) while
   **giving up the "single durable substrate" claim** (now object storage for
   data + a coordinator for metadata). That trade was already anticipated by
   ADR-004's fallback decision tree; the operator has now chosen it as the default,
   not a fallback.

## How this resolves the open re-review findings

- **TD-008 B1 (EOS cross-coordinator atomicity):** the offset commit, txn markers,
  and txn-state transition are **one Postgres transaction** — genuinely atomic.
  The `__fjord_groups`-as-participant dance is unnecessary.
- **B2 (LSO under concurrent txns):** the coordinator stores the per-partition set
  of open-transaction first-offsets; `LSO = min(remaining)` is a trivial query.
- **B3/B4 (fencing, marker idempotency):** producer epoch and a per-`transactional.id`
  monotonic txn sequence live in coordinator rows; fencing and exactly-once marker
  application are row-locked transactional updates.
- **SPIKE-001:** re-pointed from "object-log internal-topic append latency" to
  "coordinator sequence/commit latency + throughput per backend" (Postgres
  expected to pass comfortably; etcd/Dragonfly characterized).
- **N-B2 failover:** dissolved — brokers are stateless; no per-shard takeover
  replay on the produce path.
- **N-B1/N-B4 residual "leaderless/owner" contradictions:** the model is now
  cleanly "stateless brokers + central coordinator"; the ADR-005/ADR-003/TD-003
  cleanup (below) updates to this single consistent model rather than the
  single-writer-per-shard intermediate.

## Consequences

### Positive

- EOS and `read_committed` become straightforwardly correct (transactional store).
- Latency floor improves to `object_PUT + ms-class coordinator commit` — *upside*
  vs the accepted cost/ops bar; fjord may now be latency-competitive, though the
  Phase-4 bar stays the conservative cost/ops one (N-B3) unless re-decided.
- Stateless brokers restore trivial scaling/failover and simpler operations.
- Pluggability lets operators match the coordinator to their environment and lets
  fjord pressure-test the abstraction across very different stores.

### Negative / Risks

- **Gives up the single-substrate story:** a coordinator is now a required
  component to operate (HA, backup, sizing). Mitigated by defaulting to Postgres
  (ubiquitous) and being self-hosted.
- The coordinator is the new throughput/availability bottleneck and SPOF-class
  dependency; its HA (Postgres replication/failover, etcd quorum, Dragonfly
  persistence) must be specified per backend and is now the system's availability
  floor.
- Sequencing throughput per backend must be benchmarked (esp. etcd/Dragonfly);
  the `CoordinatorStore` trait must declare consistency + durability capabilities
  so unsuitable configs are rejected, not silently wrong.
- Cascade of spec revisions (see Follow-ups).

## Follow-ups (spec cascade)

- New TD: `CoordinatorStore` trait (operations, consistency/durability capability
  struct, conformance suite) + the Postgres reference schema (tables for
  metadata, sequencing/index, producer state, group state, txn state).
- Revise TD-007 (state now lives in the coordinator), TD-008 (EOS commit = one
  coordinator transaction; B1–B4 resolutions folded in), ADR-007 (reframe: the
  per-shard owner is superseded; brokers stateless), ADR-004 (mark superseded;
  object-log path becomes optional backend), ADR-005/ADR-003/TD-003/SPIKE-001
  (purge old single-writer/leaderless text → "stateless brokers + coordinator").
- Update product-vision differentiation: "self-hosted, pluggable coordinator
  (default Postgres) — not a hosted control plane," and drop "single durable
  substrate."
- TP-003: add CoordinatorStore conformance + per-backend perf to the oracle set;
  re-point SPIKE-001.

## Alternatives Considered

- **Object-log internal topics as the default sequencer (prior ADR-004 bet):**
  demoted to optional backend — latency gamble + no atomic multi-key commit.
- **Hosted/managed metadata service (WarpStream DynamoDB):** rejected — violates
  the no-hosted-control-plane differentiator. A self-hosted coordinator gives the
  same architectural benefit without the SaaS dependency.
- **Per-shard Raft (Redpanda model):** rejected as default — a bespoke consensus
  system to operate; reusing a Postgres/etcd the operator already knows is simpler.
  Remains a possible future backend behind the trait.
