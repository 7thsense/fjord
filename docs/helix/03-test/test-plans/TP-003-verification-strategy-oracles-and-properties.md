---
ddx:
  id: tp-verification-strategy-oracles-and-properties
  depends_on:
    - tp-kafka-compatibility-and-performance
    - tp-implementation-increments
    - adr-diskless-object-storage-architecture
    - adr-tail-latency-mitigation-as-cost-control
    - td-multiplexed-write-path-and-sequencing
    - td-fetch-read-path-and-cache
    - api-kafka-compatibility-surface
    - prd
---

# TP-003: Verification Strategy — External Oracles, Property-Based, and Differential Testing

## Why this plan exists

TP-001 (compatibility + performance + fault matrix) and TP-002 (increment
gates) define *what* must pass. TP-003 defines *how we know it passes without
trusting our own assertions*: every correctness claim is checked against an
**external oracle** (a known-good system or an independently-derived model), and
the diskless re-baseline (ADR-005/006, TD-005/006) introduces concurrency and
durability invariants that example-based tests cannot cover. The Phase-4 stop
condition ("prove Kafka parity, equal/better performance, simpler/cheaper") is
*defined* by the gates in this plan.

**Governing principle:** we do not assert behavior is Kafka-correct; we *diff it
against Kafka* and *check invariants an adversary picked*. These suites run
**continuously, every increment**, not as an end-phase pass.

## Oracle inventory

| ID | Oracle | Kind | Catches |
|----|--------|------|---------|
| O1 | Apache Kafka, single-node **KRaft** via Testcontainers (`apache/kafka-native`) | Reference impl (primary) | Any observable divergence in records/offsets/error-codes/watermarks |
| O2 | Redpanda (testcontainers image) | Reference impl (second) | Protocol-edge disagreements Kafka alone hides; two-impl triangulation |
| O3 | Client matrix: Java (canonical), librdkafka (kcat), franz-go | Independent spec oracles | Client-specific framing/partitioning/error handling; flexible-version negotiation |
| O4 | In-memory append-log **model** (fjord-owned, simple) | Derived model | Offset/visibility logic bugs, via proptest state machine |
| O5 | **Jepsen** `jepsen.tests.kafka` workload + Elle | Consistency oracle | Lost writes, aborted reads, duplicates, offset non-monotonicity, G0/G1c/G2 cycles |
| O6 | `heimq-testkit` per-trait conformance suites (scoped, see below) | Contract oracle | Conformance of the heimq traits fjord actually implements |

> **O6 scope (AR-2026-06-14b residue of B-5).** Under ADR-007/S1 the produce
> path does **not** route through heimq's `LogBackend`/`PartitionLog::append`
> (offset assignment lives in fjord's sequencer above the traits). So O6 conforms
> only the heimq traits fjord still uses on the serve/read side (and any retained
> wire/handler scaffolding); it does **not** cover the multiplexed write/commit
> path. That path is covered by a **fjord-native conformance suite** for the
> buffer / shard-commit / index / sequencing interfaces (the D-suite + DST). O6
> green must not be read as write-path coverage.

Config-default differences across O1–O3 (e.g. librdkafka time-based vs Java
size-based sticky partitioning) are normalized before diffing; a divergence is
flagged only after config is reconciled.

## Differential parity harness (O1/O2/O3)

The spine of the strategy, modeled on heimq's FEAT-003 parity harness.

- **Mechanism:** drive an identical, recorded workload (produce/fetch/commit/
  admin sequences, generated — see PBT below) against fjord and against the
  reference impl(s) through the same client. Capture responses; **canonicalize**
  (normalize timestamps, broker/host ids, throttle fields, and the documented
  expected-divergence set); assert byte/semantic equivalence of records,
  assigned offsets, error codes, and watermark fields (HW, LSO, log-start,
  leader epoch).
- **Expected-divergence register** (annotated, not silent — heimq parking-lot
  pattern): (1) latency — fjord's object-storage floor differs by construction
  (ADR-006), so timing is excluded from parity and checked separately by perf;
  (2) `acks=1` upgraded-to-durable (TD-003) is a deliberate, documented
  behavioral difference; (3) `__consumer_offsets`/internal-topic surfacing
  differs (fjord uses `__fjord_*`); (4) KIP-890 transaction write-cycle (below).
- **Gate:** zero unexplained diffs on the supported surface to merge. New
  divergences must be either fixed or added to the register with a reproducer
  and a rationale.

> **Falsifiability constraints (AR-2026-06-14 B-10).** Without limits, the
> register + an adjustable "supported surface" make "parity" unfalsifiable —
> fjord could register away any diff. Therefore:
> 1. **Categorically ineligible** for the register (these are parity *failures*,
>    never "expected divergences"): any behavior **observable by a standard,
>    unmodified Kafka client on the produce / fetch / consumer-group-commit
>    surface without configuration changes** — wrong offsets, wrong records,
>    wrong error codes, lost/duplicated acknowledged data, ordering violations,
>    or visibility (HW/LSO) differences. `acks=1`-upgrade is the *one* permitted
>    semantic exception and only because it is louder-durability, never
>    weaker-than-advertised, and is disclosed in API-001.
> 2. The **supported surface is frozen in API-001**, not adjustable per CI run;
>    narrowing it requires an API-001 change with review, not a test-config edit.
> 3. Every permitted divergence has an executable reproducer in the external-
>    oracle suite or an evidence-producing fault/performance harness; any
>    client-observable diff outside those tests is a failure.
> 4. Register **additions require external sign-off** (not the author of the
>    diverging code) and are capped/reviewed — growth of the register is itself a
>    parity-erosion signal tracked over time.

## Property-based & model-based testing (O4, O5-invariants)

**Engine:** `proptest` (state-machine module, eqc_statem-style) — richer
shrinking than quickcheck, integrated stateful testing.

- **Model-based:** an abstract in-memory log model is the oracle. Generate
  random transition sequences (produce to random partitions with random
  acks/keys/producer-ids, fetch from random offsets, commit/fetch offsets,
  rebalance) and assert fjord's observable result equals the model's, shrinking
  to a minimal failing sequence on divergence.
- **Invariant suite (the Jepsen property set, encoded as proptest + checked by
  O5 in anger):**
  - **Lost writes** — every acknowledged send is eventually polled.
  - **Aborted reads (G1a)** — failed/uncommitted writes never appear in
    successful reads.
  - **Duplicates** — idempotent producer never yields a value at two offsets.
  - **Offset monotonicity** — all flavors: poll-skip, internal poll/send
    non-monotonic, per-process non-monotonic sends.
  - **Unique offset per value** — one value ↔ one offset per key.
  - **Cycle detection G0/G1c/G2** — via Elle dependency graphs (O5).
- **Known protocol artifact (not our bug):** under `read_committed`, one
  transaction's writes can interleave into another's (KIP-890) — confirmed by
  Jepsen across Kafka, Redpanda, **and** Bufstream. The suite asserts fjord's
  behavior *matches the reference impls*, not an idealized serializability the
  protocol itself does not provide.

## Diskless correctness suite (new invariants from TD-005/TD-006)

These have no analog in classic-broker test plans; they verify the
sequence-at-commit, multiplexed, leaderless design.

| ID | Property | Method | Oracle |
|----|----------|--------|--------|
| D1 | Concurrent-writer offset monotonicity & uniqueness | 2–4 nodes flush L0 objects with overlapping partitions; assert contiguous/unique/monotonic offsets | O4 + DST |
| D2 | Idempotency-at-commit (retroactive tombstone) | Duplicate base-sequence batch drops from partition, returns prior offset; gap → `OUT_OF_ORDER_SEQUENCE_NUMBER` | O1 diff + O4 |
| D3 | Crash between PUT and commit | Kill writer after L0 PUT, before commit: no acked write lost, orphan L0 gets no index entry, replay excludes orphan | DST |
| D4 | Compaction crash-safety (L0→L1) | Kill mid-compaction: reads stay correct, no offset gap/dup across the index switch | DST |
| D5 | Read-your-writes | Record fetchable immediately after its `acks=all` ack (shared commit lin-point) | O1 diff |
| D6 | GET-count invariance | N consumers×1 partition and 1 consumer×N partitions both bounded by chunk count, not N | cost counters |
| D7 | PUT-count invariance | Fixed-MB workload across 1 vs 1000 partitions yields ≈equal PUTs (multiplexing) | cost counters |
| D8 | Sequencing is the lin-point | Visibility/ordering follows metadata-commit order, never object-PUT order | O4 + DST |
| D9 | Compaction keeps up with ingest (AR-W-b) | Sustained ingest at target MB/s: assert per-partition L0-object count and total L0 backlog stay bounded; replay time (takeover) stays within SPIKE-001's 5 s bar; express-tier object count does not grow unbounded | perf harness |
| D10 | Lease fencing (re-scoped for ADR-008) | A broker whose membership lease has expired (partitioned away) has its `commit_object`/`end_txn` rejected by the coordinator (lease check in the transaction); only lease-valid brokers mutate state | O7 + DST |
| D11 | Leader-epoch churn (re-scoped for ADR-008) | A Java consumer fetching continuously through 10 coordinator-driven assignment changes (each bumps `partition_leader_epoch`) in 30 s: `offset_for_leader_epoch` answers consistently, no spurious truncation, no `LogTruncationException`, no stuck consumer, correct final position | O1/O3 |

GET/PUT-count invariance (D6/D7) and D9 must count **all** PUT-class ops — L0
data PUTs (incl. zonal-quorum copies), the durable shard-commit append, and L1
compaction PUTs — when reporting per-MB cost (AR-N1), and add L0-fan-out (number
of un-compacted L0 objects touching a partition) as an explicit read-cost axis.

## Deterministic Simulation Testing (DST) — the concurrency confidence engine

Concurrency/durability bugs (D1, D3, D4, D8) are non-reproducible under wall-
clock tests. fjord runs the sequencing/commit/compaction paths under a
**deterministic harness**: a seeded scheduler and a mock object store with
controllable (injectable) PUT/GET latency, reordering, transient failures, and
partial writes. A failing seed reproduces exactly. DST is the primary oracle for
the diskless invariants and runs every increment; failing seeds are captured as
regression fixtures. (Prior art: diskless-Kafka DST writeups; this is a
fjord-owned harness, not an off-the-shelf crate.)

> **Soundness (AR-2026-06-14 W-d).** DST and the O4 model are written by the same
> team as fjord, so a shared wrong assumption (e.g. about `put_if_absent`/CAS
> failure modes or owner-epoch fencing under partial writes) could make DST green
> while production is wrong. Guards:
> 1. The DST **mock object store derives its CAS / put-if-absent / partial-write
>    semantics from object-log's CONTRACT-002**, not from fjord's expectations,
>    and a conformance test asserts the mock matches a **real target store** (not
>    MinIO) for those ops before DST is trusted as a merge gate.
> 2. **DST gating is staged:** until that real-store validation lands, DST blocks
>    merges only for invariants independent of CAS/partial-write modeling (D5
>    read-your-writes, D8 lin-point ordering); the contention/fencing invariants
>    (D1, D3, D10) are advisory until the mock is validated.
> 3. The O4 model is cross-checked against **real Kafka (O1)**, not only against
>    fjord, so a class of shared model/impl bugs surfaces as an O1 diff even when
>    the O4 comparison passes — breaking the circularity on the Kafka-observable
>    surface.

## Performance & cost verification (feeds ADR-006 targets / Phase-4)

- **Tools:** OpenMessaging Benchmark (datastax fork) for cross-system numbers;
  `kafka-producer-perf-test` / `kafka-consumer-perf-test` for single-dimension
  checks; same harness run against fjord, Kafka, and Redpanda on equivalent
  hardware.
- **Metrics:** produce **p50/p99/p999**, end-to-end latency, fetch latency,
  throughput-at-fixed-p99-SLA; per ADR-006 profile (`throughput`/`balanced`/
  `low-latency`).
- **Cost metrics:** PUT/GET count per MB ingested/consumed (must be
  partition-count-independent — D6/D7), inter-AZ bytes (target zero),
  $/GB-ingested, $/GB-month-retained vs a classic-Kafka baseline.
- **Regression posture:** numbers tracked over time with the TP-001 evidence
  block (commit SHA, object store/region, node/instance, client config, etc.);
  a regression beyond a threshold fails the perf gate.

## Continuous cadence (the "ongoing basis" requirement)

| Suite | Cadence | Merge gate? |
|-------|---------|-------------|
| Unit + proptest model (O4) + diskless DST (D1–D10 via DST; D1/D3/D10 gate only after real-store mock validation, W-d) + D11 via O1/O3 | every commit/CI | yes |
| heimq-testkit conformance (O6) | every commit/CI | yes |
| Differential parity vs Kafka (O1) on supported surface | every commit/CI (containerized) | yes |
| Client matrix (O3) + Redpanda diff (O2) | every commit or nightly | yes (nightly blocks release) |
| Jepsen (O5) | nightly / pre-release | release gate |
| OMB perf + cost (per profile) | nightly + pre-release | release gate (regression) |

Implementation increments (TP-002) layer onto this: each increment turns on the
subset of suites its surface supports, and **no increment is "done" until its
oracle diff is green**.

## Phase-4 stop condition (made testable)

The loop terminates only when, with recorded evidence:
1. **Parity:** zero unexplained diffs vs Kafka (O1) and Redpanda (O2) on the
   API-001-frozen supported surface (B-10); Jepsen (O5) clean except the
   registered KIP-890 artifact; all **D1–D11** green (D1–D10 under DST, with
   D1/D3/D10 requiring the real-store-validated DST mock per W-d; D11 via O1/O3);
   and **O7 CoordinatorStore conformance** green for the configured backend.
2. **Performance (scoped baselines, N-B3 / 2026-06-15 decision):** produce p99
   meets fjord's **own SPIKE-001-derived absolute target** per ADR-006 profile.
   fjord does **not** claim latency parity with WarpStream; that is explicitly
   **not** a stop condition. Throughput-at-p99-SLA meets/beats object-storage-
   class references at fjord's own p99 bound. Failover produce-unavailability is
   within the ADR-007 budget (N-B2).
3. **Cost/simplicity (the primary "better" claim):** PUT/GET partition-count-
   independent (D6/D7), zero inter-AZ replication, no local persistent disk, no
   consensus system; documented $/GB advantage vs classic Kafka AND operational-
   surface parity-or-better vs WarpStream-class **without** a hosted control
   plane.
4. **Full Kafka capability (N-B5):** EOS/transactions pass the parity + Jepsen
   transactional invariants (per the EOS TD); structurally-excluded features are
   declared in the API-001 capability matrix with client-visible errors, not
   silent gaps.

## Implementation Closure Beads

- `bead:jepsen-history`: run Jepsen's Kafka workload or an equivalent history
  checker against fjord's Kafka surface for lost acknowledged writes, duplicates,
  offset monotonicity, aborted reads, stuck clients, and transactional anomalies.
- `bead:dst-real-store-model`: validate the DST mock object-store semantics
  against the real target store for CAS, put-if-absent, partial writes, transient
  errors, and read-after-write behavior before using those modeled faults as
  merge evidence.
- `bead:eos-backend-matrix`: run the TD-008 EOS invariants against every
  `CoordinatorStore` backend; transactional backends such as Postgres perform
  `end_txn` as one coordinator transaction per COORD-001.
- Two-impl diff assumes Redpanda parity with Kafka on the surface; where they
  disagree, Kafka (O1) is authoritative.

> **CoordinatorStore oracle (O7, ADR-008/COORD-001).** Add the **CoordinatorStore
> conformance + per-backend perf suite** (COORD-001 §Conformance) to the oracle
> set, as a merge gate for the default Postgres backend and a release gate for
> etcd/Dragonfly: linearizable `commit_object`, atomic `end_txn` under injected
> crash, idempotency/fencing, group-generation monotonicity, lease fencing,
> durability-survives-restart, and `commit_object`/`end_txn` p50/p99/throughput
> per backend (this is where re-scoped SPIKE-001 lands). Note: the ADR-007
> failover-unavailability divergence is **no longer** in the expected-divergence
> register — ADR-008's stateless brokers dissolve it (broker death loses no
> durable state); the coordinator's HA is the availability floor instead.

## Implemented differential tests

`crates/fjord-heimq-backend/tests/differential.rs` drives fjord and Apache
Kafka through the same standard client paths and compares client-visible
results. The suite covers single-partition produce/fetch, explicit
multi-partition produce/fetch, low/high watermarks, committed-offset resume, and
idempotent-producer sequencing. Expansion beads above close the remaining
oracle surfaces by adding Redpanda, EOS histories, durable backend faults, and
long-run performance evidence.
