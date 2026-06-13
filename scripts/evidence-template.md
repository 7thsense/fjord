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
| Java Kafka client | (not tested — use rdkafka conformance instead) |

## Declared Compatibility Level

**L1** — Produce (acks=all), Fetch, Metadata, ApiVersions, Consumer group offsets

APIs NOT supported at this level:
- Transactions / EOS
- Admin API (CreateTopics, DeleteTopics via admin client)
- Log compaction
- Quota enforcement
- ACLs

## Test Results

### TP-001 Critical Paths

| ID | Scenario | Pass/Fail | Notes |
|----|----------|-----------|-------|
| T1 | ApiVersions | | |
| T2 | Metadata | | |
| T3 | Produce acks=all | | |
| T4 | Produce/fetch round-trip | | |
| T5 | Node loss after ack | N/A (single-node) | |
| T6 | OffsetCommit + node loss | | see kafka_smoke restart test |
| T7 | Consumer group rebalance | | |
| T8 | Corrupt segment fixture | | conformance test |
| T9 | Object-store transient failure | not tested | |
| T10 | Tiny-object rejection | | conformance test |
| T11 | Out-of-order object writes | not tested | |
| T12 | Owner reassignment | not tested | |
| T13 | acks=0/1/all | partial | acks=all tested |
| T14 | Fetch watermark fields | | |
| T15 | Epoch coherence | not tested | |
| T16 | Metrics scrape | not tested | Prometheus not wired |

### cargo test Suite

```
# Run from /path/to/fjord:
cargo test -p fjord-object-log
cargo test -p fjord-broker
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
| Object PUT count | not measured | Prometheus not enabled |
| Object GET count | not measured | Prometheus not enabled |
| Cache hit rate | not measured | |

## Known Gaps

- Prometheus metrics endpoint not yet wired; object PUT/GET counts and cache
  hit rate cannot be reported without it.
- Transactions (EOS) not supported.
- Single-node only; multi-node metadata routing not tested at this level.
- `acks=0` behavior not validated.
