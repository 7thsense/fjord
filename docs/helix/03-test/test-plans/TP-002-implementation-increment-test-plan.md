---
ddx:
  id: tp-implementation-increments
  depends_on:
    - td-kafka-protocol-gateway
    - td-metadata-routing-coordination
    - td-object-log-data-plane
---

# TP-002: Implementation Increment Test Plan

## Purpose

Give each implementation increment a concrete verification gate before Fjord
claims any Kafka compatibility level.

## Increment Gates

| Increment | Required Tests |
|-----------|----------------|
| Protocol skeleton | unit tests for frame parsing, header versions, ApiVersions, unsupported API errors |
| Metadata skeleton | `kcat -L` smoke test plus Java client metadata test |
| Produce skeleton | produce request decode/response encode fixture without durable claim |
| object-log produce | Java client produce with `acks=all` returns offsets only after object-log commit |
| Fetch skeleton | fetch fixture returns records by offset from in-memory object-log |
| Consumer smoke | Java client assigned consumer reads produced records |
| Group coordinator | Java consumer group commit/restart/rebalance tests |
| Failure harness | node kill before/after ack, cache loss, corrupt segment, object-store timeout |

## Shared Fixture Targets

- Java Kafka client.
- librdkafka/kcat.
- kafka-go or sarama.
- Kafka CLI performance tools.
- object-log conformance suite.

## Evidence

Every compatibility milestone records:

- supported API/version matrix,
- client versions,
- object-log commit SHA/version,
- metadata backend mode,
- object store mode,
- test commands and logs,
- known unsupported APIs.

