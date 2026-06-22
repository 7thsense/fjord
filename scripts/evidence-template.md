# Fjord Compatibility Evidence — [DATE]

## Configuration

| Field | Value |
|-------|-------|
| fjord/heimq version | `git rev-parse HEAD` output |
| object-log version | (from Cargo.lock) |
| object-log crate | `fjord-object-log v0.1.0` |
| backend configuration | `memory://` or `local:///path` |
| object store mode | `MemoryObjectStore` / `LocalObjectStore` |
| node count | 1 (single-node) |
| batch profile | default (min_segment_bytes=64) |
| test date | |
| test environment | |

## Client Versions

| Client | Version |
|--------|---------|
| rdkafka (librdkafka) | |
| kcat | |
| kafka-go | |
| Java Kafka client | |

## Declared Compatibility Level

Use API-001 as the source of truth for the declared surface. Expected
divergences must be named with reproducer, rationale, and test coverage;
unregistered client-observable diffs are evidence failures, not notes.

## Test Results

### TP-001 Critical Paths

| ID | Scenario | Pass/Fail | Notes |
|----|----------|-----------|-------|
| T1 | ApiVersions | | |
| T2 | Metadata | | |
| T3 | Produce acks=all | | |
| T4 | Produce/fetch round-trip | | |
| T5 | Node loss after ack | | |
| T6 | OffsetCommit + node loss | | see kafka_smoke restart test |
| T7 | Consumer group rebalance | | |
| T8 | Corrupt segment fixture | | conformance test |
| T9 | Object-store transient failure | | |
| T10 | Tiny-object rejection | | conformance test |
| T11 | Out-of-order object writes | | |
| T12 | Owner reassignment | | |
| T13 | acks=0/1/all | | |
| T14 | Fetch watermark fields | | |
| T15 | Epoch coherence | | |
| T16 | Metrics scrape | | |

### cargo test Suite

```
# Run from /path/to/fjord:
cargo test --workspace
cargo test -p fjord-heimq-backend --test differential \
  differential_single_partition_matches_real_kafka -- --ignored --nocapture
```

Expected: all tests pass.

Actual result (paste output here):

```
```

### compat-smoke.sh

```
# Prerequisites: kcat installed, fjord server running
./scripts/compat-smoke.sh 127.0.0.1:9092
```

Actual result (paste output here):

```
```

### perf-smoke.sh

```
# Prerequisites: kcat installed, fjord server running
./scripts/perf-smoke.sh 127.0.0.1:9092 10000 1024
```

Actual result (paste output here):

```
```

## Performance Summary

| Metric | Value | Notes |
|--------|-------|-------|
| Produce records/sec | | via kcat, 1KB records |
| Fetch records/sec | | sequential |
| p99 latency | | estimated from wall-clock |
| Object PUT count | | |
| Object GET count | | |
| Cache hit rate | | |

## Closure Beads

- `bead:metrics-evidence`: object PUT/GET counts, cache hit rate, latency
  percentiles, and consumer lag are captured in this evidence bundle.
- `bead:eos-evidence`: transactions, `read_committed`, and abort filtering are
  covered by external-oracle histories.
- `bead:acks-evidence`: `acks=0`, `acks=1`, and `acks=all` behavior is covered
  by standard-client tests.
