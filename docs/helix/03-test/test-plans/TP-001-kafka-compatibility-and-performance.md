---
ddx:
  id: tp-kafka-compatibility-and-performance
  depends_on:
    - prd
    - api-kafka-compatibility-surface
    - adr-fjord-as-kafka-compatible-object-log-system
---

# TP-001: Kafka Compatibility and Performance Test Plan

## Testing Strategy

fjord must be tested like a Kafka-compatible system: with standard clients,
standard Kafka command-line tools, protocol-level fixtures, failure injection,
and object-storage cost/performance accounting. Internal unit tests are
necessary but not sufficient.

This plan is design-time only. No implementation exists yet.

## Test Levels

| Level | Purpose | Gate |
|-------|---------|------|
| Protocol conformance | Validate supported Kafka API versions, framing, errors, and compatibility matrix | Required before any compatibility claim |
| Client workflow | Prove Java Kafka client, librdkafka, and kafka-go workflows | Required for L1/L2 |
| object-log integration | Prove durable object-storage ack, replay, indexes, and corruption handling | Required before produce ack support |
| Metadata routing | Prove client-visible leaders, synthetic leaders, or load-balanced metadata responses behave with real clients | Required before L1 |
| Consumer group correctness | Prove committed offsets, rebalances, heartbeat/session behavior, and node-loss recovery | Required for L2 |
| Performance/cost | Measure produce/fetch throughput, p95/p99 latency, object operations, segment size, cache hit rate | Required before production profile |
| Fault injection | Prove node kill, cache loss, object-store errors, metadata conflicts, stale epochs, corrupt segment handling | Required before production profile |

## Standard Kafka Tooling

| Tool | Use |
|------|-----|
| `kafka-producer-perf-test.sh` | Produce throughput, batching, latency, ack profile comparisons |
| `kafka-consumer-perf-test.sh` | Fetch throughput and consumer lag behavior |
| `kafka-console-producer.sh` / `kafka-console-consumer.sh` | Smoke tests for ordinary client workflows |
| `kafka-topics.sh` | Topic create/list/describe/delete compatibility once admin APIs are in scope |
| `kafka-consumer-groups.sh` | Group membership and offset visibility once groups are in scope |
| `kcat` | Lightweight produce/fetch/metadata smoke tests from librdkafka |
| Java Kafka client integration tests | Canonical protocol/client compatibility |
| `kafka-go` or `sarama` tests | Non-JVM client compatibility |

## Coverage Requirements

Every PRD functional requirement maps to at least one scenario below or is
explicitly listed as design-gated in the Coverage Notes.

### Critical Paths (P0)

| ID | Scenario | Requirement Coverage | Expected Result |
|----|----------|----------------------|-----------------|
| T1 | ApiVersions with supported and unsupported versions | FR-1, FR-3, FR-4 | Version list matches the flexible-version floors in TD-001; below-floor requests get `UNSUPPORTED_VERSION`; legacy ApiVersions requests (v0+) still negotiate |
| T2 | Metadata for existing and missing topics | FR-2, FR-19 | Single owner per partition per ADR-003; missing topics return expected errors; clients route to the owner |
| T3 | Produce batch with `acks=all` | FR-6, FR-7, FR-9, FR-23 | Offsets returned only after object-log durable commit |
| T4 | Produce/fetch round trip | FR-5, FR-11, FR-13 | Consumer reads records in partition offset order |
| T5 | Node loss after acknowledged produce | FR-7, FR-21, FR-25, FR-28 | Replacement node fetches acknowledged records from object storage |
| T6 | OffsetCommit then node loss | FR-15, FR-17, FR-18 | Committed offset is preserved and fetchable |
| T7 | Consumer group rebalance | FR-16 | Assignment converges and committed offsets remain valid |
| T8 | Corrupt segment fixture | FR-13, FR-25, FR-30 | Fetch/replay fails closed with corruption error |
| T9 | Object-store transient failure | FR-4, FR-30 | Produce/fetch return retryable Kafka-compatible errors; no false ack |
| T10 | Tiny-object production config | FR-24, FR-27 | Config is rejected or marked test-only |
| T11 | Out-of-order object writes | FR-31 | Fetch follows metadata/manifest ordering, not object creation order |
| T12 | Owner reassignment and stale routing | FR-19, FR-32 | Requests to non-owners return `NOT_LEADER_OR_FOLLOWER`; clients refresh metadata and reroute to the new owner (ADR-003); epoch persisted before announcement |
| T13 | Produce with `acks=0`, `acks=1`, and `acks=all` | FR-8 | `acks=all` and `acks=1` acknowledge only after object-log durable commit (`acks=1` upgraded by default per API-001); `acks=0` returns no committed offsets; reject-mode profile refuses `acks=1` |
| T14 | Fetch response watermark fields | FR-14 | High watermark, log start offset, last stable offset, and leader epoch match documented object-storage-backed semantics |
| T15 | Epoch coherence after reassignment | FR-20 | Leader epoch, partition epoch, and manifest state agree after node failure and reassignment; stale-epoch writes are fenced |
| T16 | Metrics surface scrape | FR-29 | Produce/fetch latency, object operation counts, segment size, cache hit rate, rebalance, and metadata error metrics are exposed |

### Coverage Notes

FR-10 (idempotent producer design), FR-12 (fetch index/cache design), FR-22
(metadata backend boundary), and FR-32 (durable metadata placement) are
design-gated requirements: they are satisfied by approved design artifacts
(follow-up ADRs and TD-002) before their implementation scenarios are added
here. FR-26 (object-log neutrality) is enforced by object-log's own
conformance suite and dependency rules rather than a fjord runtime scenario.

## Acceptance Criteria Layer Allocation

N/A at this stage: fjord has no user-story artifacts yet, so there are no
`US-n-ACm` IDs to allocate across test layers. When user stories are framed
for L1/L2 delivery, each acceptance criterion must be allocated to a layer
here and covered by a test citing `@covers US-<n>-AC<m>`.

## Performance Profiles

| Profile | Purpose | Metrics |
|---------|---------|---------|
| Cost-optimized S3 Standard | Large batches, higher ack latency, low object cost | records/sec, p95/p99 ack latency, PUTs per million records, GETs per GB fetched |
| Lower-latency object store | Smaller batches or faster object tier | latency/cost delta vs cost profile |
| Cache-heavy fetch | Hot consumers over recent data | cache hit rate, fetch p99, object GET rate |
| Cold replay | Consumer starts from old offset | object GET throughput, index efficiency, recovery time |

## Fault Matrix

| Fault | Expected Behavior |
|-------|-------------------|
| fjord node killed before produce ack | No acknowledged record is required to survive; client retry semantics apply |
| fjord node killed after produce ack | Record survives through object-log/object storage |
| Local cache deleted | Service remains correct; fetch may be slower |
| Object PUT timeout | No committed ack unless object-log commit boundary completed |
| Metadata conflict | One writer/coordinator decision wins; clients get retryable stale metadata or coordinator errors |
| Stale producer epoch | Duplicate or stale writes are rejected once idempotent producers are in scope |
| Corrupt object segment | Segment is not served as valid data |

## Evidence Requirements

Every performance claim must record:

- commit SHA,
- fjord compatibility level,
- object-log version,
- object store and region/provider,
- metadata backend,
- node count and instance type,
- topic/partition count,
- producer/consumer client configuration,
- record size and compression,
- batch profile,
- retention/compaction settings,
- full command line for Kafka tools.

## Known Gaps

- No implementation exists yet.
- The metadata backend direction is decided (ADR-004) but unconfirmed until
  SPIKE-001 passes its latency bars.
- Leader semantics are decided (ADR-003, emulated single leader for L1/L2).
- The build/no-build differentiation review against WarpStream-class systems is
  not complete (gates defined in the build/no-build validation checklist;
  first review before M3).
- Transactions and read-committed isolation are not designed.
- Security tests await the TLS/SASL/ACL design.
