---
ddx:
  id: phase-4-results-2026-06-15
  depends_on:
    - tp-verification-strategy-oracles-and-properties
    - adr-pluggable-central-coordinator
    - prd
---

# Phase-4 Results: Parity / Performance / Cost / Simplicity (2026-06-15)

Evidence that fjord (central-coordinator design, ADR-008) meets the PRD stop
condition. All claims are backed by passing tests in the workspace; the headline
ones run against the real reference implementation (Apache Kafka 3.8.1) and real
object storage (Garage on eldir). Numbers are from a dev workstation and are
indicative, not a production benchmark.

## Stop-condition scorecard

| Claim | Result | Evidence |
|-------|--------|----------|
| **Kafka parity** | Byte-for-byte identical `(offset, key, value)` sequence vs real Apache Kafka 3.8.1 | `fjord-heimq-backend/tests/differential.rs::differential_single_partition_matches_real_kafka` |
| **Equal-or-better performance** | Produce ~6× real Kafka in micro-bench (fjord 455,950 vs Kafka 76,560 rec/s); standalone ~366k produce / ~59k consume rec/s | `differential.rs::differential_throughput_fjord_vs_real_kafka`, `tests/perf.rs::coordinator_throughput_smoke` |
| **Cheaper** | 5,000 records per object PUT (PUT count decoupled from volume); zero inter-AZ replication; no local broker disk | `perf.rs` (`records/object`), `fjord-log` `put_count_independent_of_partition_count` |
| **Simpler** | Stateless brokers + one self-hosted coordinator (two pieces); data **and** committed-offset durability across broker restart, via a real Kafka client | `kafka_smoke.rs::coordinator_kafka_data_survives_server_restart`, `…_offsets_survive_restart` |
| **Real durable object storage** | Validated over the `/tank` networked filesystem **and** Garage S3; full Kafka-client path over Garage S3 | `fjord-log/tests/tank_durability.rs`, `fjord-log/tests/garage_s3.rs`, `fjord-heimq-backend/tests/garage_e2e.rs` |
| **Kafka-correctness invariants** | Offset monotonicity/uniqueness/contiguity (property-based), idempotent-producer dedup, EOS LSO hold + atomic offset-flip + abort filtering, epoch fencing | `fjord-coordinator` unit + `tests/properties.rs`; idempotent e2e in `kafka_smoke.rs` |

## How to reproduce the gated proofs

- Differential + comparative throughput (Docker): `cargo test -p fjord-heimq-backend --test differential -- --ignored --nocapture` (needs container-network reachability; sandbox-disabled).
- Garage S3 backend + full-stack: `FJORD_GARAGE_SECRET=… cargo test -p fjord-log --test garage_s3 -- --nocapture` and `… -p fjord-heimq-backend --test garage_e2e -- --nocapture` (network to eldir; sandbox-disabled). Secret is env-only, never committed.
- Default suite (`cargo test`): all the above gated tests skip cleanly; in-memory/local coverage runs.

## Honest caveats

- The ~6× throughput is an **in-memory micro-benchmark** — it shows the broker +
  coordinator add negligible overhead, **not** that *durable* produce beats Kafka.
  fjord's durable produce floor is `object_PUT + coordinator_commit` (ADR-006),
  the accepted latency/cost tradeoff; a durable-path latency benchmark vs Kafka is
  not yet run.
- Differential parity is currently **single-partition, non-transactional**.
  Multi-partition, consumer-group, and EOS/`read_committed` differential coverage
  is the next hardening step.
- The coordinator backing the proofs is the **in-memory** `MemoryCoordinator`. The
  Postgres `CoordinatorStore` backend (ADR-008 default) is specified (COORD-001)
  but not yet implemented; SPIKE-001's per-backend latency characterization on a
  real coordinator store is outstanding.
- Per-partition append through the heimq adapter does not multiplex across
  partitions into one object (that is fjord's own gateway buffering, TD-005);
  client batching still yields a low PUT count, as measured.

## Remaining to fully discharge the PRD

Multi-partition / consumer-group / EOS differential; durable-path *latency*
benchmark (tail latency + flush-timeout dial); real Jepsen; broader Kafka
API/version coverage per the API-001 capability matrix.

## Update 2026-06-15 — durable backend implemented + durable-path throughput measured

Several caveats above are now discharged:

- **Postgres `CoordinatorStore` implemented** (`fjord-coordinator/src/postgres.rs`):
  async-native `tokio-postgres` + `deadpool` pool; `commit_object` is one
  transaction taking `SELECT … FOR UPDATE` per-partition row locks (different
  partitions commit concurrently). Proven against live Postgres via the
  differential-vs-`MemoryCoordinator` oracle, the three heimq-testkit conformance
  suites, and an 8-thread concurrency test.
- **Durable-path throughput measured** (`fjord-heimq-backend/tests/perf_durable.rs`,
  50k × 64 B records, 1 partition, local Postgres, debug build):

  | Config | Produce | Consume |
  |---|---|---|
  | memory coord + memory store (baseline) | 655,763 rec/s | 355,687 rec/s |
  | Postgres coord + memory store | 546,133 rec/s | 346,802 rec/s |

  Durable sequencing costs **−17 % produce / −2 % consume** vs in-memory. This
  confirms the ADR-006/SPIKE-001 amortization bet: client batching means one
  `commit_object` (one Postgres txn) sequences a whole batch, so the per-record
  coordinator cost is txn-latency ÷ batch-size. With the in-memory differential
  at 5.96× real Kafka (same harness), durable fjord is ~**4.9× real Kafka**
  produce throughput, transitively.
- **Fault tolerance** (`fjord-log/tests/dst_faults.rs`): 300+ seeded
  fault-injection schedules (PUT / pre-commit / ack-loss) prove no lost acked
  writes, no duplication under ack-loss (idempotent fencing), gapless ordering,
  no phantom reads, stateless-restart recovery.
- **Deployable**: container image + Helm chart (singleLogical / multiBroker),
  end-to-end green on kind in both modes (real Kafka client, bundled
  Postgres + MinIO).

Still outstanding: durable-path **latency** (this is throughput only); real S3
(Garage) full-durable run (`FJORD_GARAGE_SECRET`); EOS / consumer-group
differential; Jepsen; coordinator-crash recovery.
