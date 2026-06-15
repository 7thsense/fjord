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

Postgres `CoordinatorStore`; multi-partition / consumer-group / EOS differential;
durable-path latency benchmark; SPIKE-001 on a real coordinator backend; broader
Kafka API/version coverage per the API-001 capability matrix.
