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

| API | Key | Level | Notes |
|-----|-----|-------|-------|
| ApiVersions | 18 | L1 | Required first request for most clients |
| Metadata | 3 | L1 | Routes topics/partitions according to chosen leader model |
| Produce | 0 | L1 | Maps to object-log append once data plane is ready |
| Fetch | 1 | L1 | Maps to object-log read once data plane is ready |
| ListOffsets | 2 | L1 | Required by consumers for offset discovery |
| FindCoordinator | 10 | L2 | Required for groups |
| JoinGroup/SyncGroup/Heartbeat/LeaveGroup | 11/14/12/13 | L2 | Group coordinator |
| OffsetCommit/OffsetFetch | 8/9 | L2 | Durable group offsets |
| CreateTopics/DescribeConfigs | 19/32 | L3 | Admin compatibility |

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

