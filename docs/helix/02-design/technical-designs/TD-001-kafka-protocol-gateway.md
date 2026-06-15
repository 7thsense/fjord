---
ddx:
  id: td-kafka-protocol-gateway
  depends_on:
    - api-kafka-compatibility-surface
    - adr-extract-shared-kafka-wire-scaffolding
    - research-niflheim-kafka-protocol
    - prd
---

# Technical Design: TD-001 Kafka Protocol Gateway

## Scope

Build Fjord's Kafka TCP frontend in increments while preserving a clean
extraction path for shared Kafka wire scaffolding.

## Components

| Component | Responsibility |
|-----------|----------------|
| Connection | TCP/TLS stream handling, frame read/write, bounded reader-to-writer queue |
| Framing | Kafka size-prefixed frames and max-frame validation |
| Versioning | Request/response header versions and ApiVersions response |
| Registry | API key/version dispatch to handlers |
| Handlers | Product handlers for Metadata, Produce, Fetch, ListOffsets, groups, and admin APIs |
| Error mapper | Kafka-compatible error responses for unsupported versions and retryable failures |

## Initial API Matrix

Version floor (API-001 principle 5, decided 2026-06-12): each API is
supported from its **first flexible version** (KIP-482) upward. Older
non-flexible versions get `UNSUPPORTED_VERSION`, except that ApiVersions
requests must parse at v0+ because negotiation bootstraps there. The floors
below are the expected first-flexible versions; each is pinned exactly, and
validated against the fixture clients, when its handler lands.

| API | Key | Version floor | Level | Notes |
|-----|-----|---------------|-------|-------|
| ApiVersions | 18 | v3 (requests parsed from v0) | L1 | Required first request for most clients |
| Metadata | 3 | v9 | L1 | Routes topics/partitions; leader is a routing/cache-locality hint (ADR-008) |
| Produce | 0 | v9 | L1 | Broker buffers → L0 object → coordinator `commit_object` (TD-005) |
| Fetch | 1 | v12 | L1 | Coordinator `index_lookup` → object read (TD-006) |
| ListOffsets | 2 | v6 | L1 | Required by consumers for offset discovery |
| FindCoordinator | 10 | v3 | L2 | Returns the coordinator for the group (TD-007) |
| JoinGroup/SyncGroup/Heartbeat/LeaveGroup | 11/14/12/13 | v6/v4/v4/v4 | L2 | Group coordinator (TD-007) |
| OffsetCommit/OffsetFetch | 8/9 | v8/v6 | L2 | Durable group offsets |
| CreateTopics/DescribeConfigs | 19/32 | v5/v4 | L3 | Admin compatibility |

## Niflheim-Informed Design Choices

- Use `kafka-protocol` rather than hand-coded message structs.
- Preserve `Bytes` payloads through the produce path.
- Split reader and writer tasks with a bounded channel to support S3/object-log
  batch coalescing under durable write latency.
- Keep protocol handling independent from metadata, group, and storage logic.
- Build compatibility tests with Java client, kcat/librdkafka, and kafka-go.

## Handler Boundary

Handlers receive decoded header metadata, body bytes, connection auth context,
and a product context. Handlers return encoded response body bytes; the gateway
owns response headers and frame writes.

This keeps shared wire code independent from Fjord-specific state.

## Testing

- Unit tests for header version selection and frame validation.
- Protocol fixture tests for ApiVersions and unsupported version errors.
- TCP smoke tests with `kcat -L` after Metadata exists.
- Java client tests for produce/fetch once object-log integration exists.

